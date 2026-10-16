"""Unit tests for the Bronze pipeline: promotion, failure handling, idempotency.

Everything runs against an in-memory object store seeded with contract-shaped
raw objects; the Great Expectations gate runs for real on the fixture batches.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from bronze_fixtures import (
    BRONZE_BUCKET,
    CONTRACTS,
    DEFAULT_TAG,
    RAW_BUCKET,
    channel_row,
    csv_bytes,
    make_settings,
    raw_key,
    region_row,
    sales_row,
    seed_dimensions,
    seed_raw,
)

from mri.ingestion.bronze import BronzePipeline, BronzeRunResult
from mri.ingestion.config import Settings
from mri.ingestion.exceptions import UsageError
from mri.ingestion.storage import InMemoryObjectStore

PROVENANCE = {
    "source",
    "source_file",
    "source_hash",
    "ingestion_run_id",
    "ingestion_timestamp",
}


def _pipeline(
    tmp_path: Path, *, dry_run: bool = False
) -> tuple[BronzePipeline, Settings, InMemoryObjectStore]:
    settings = make_settings(tmp_path)
    store = InMemoryObjectStore()
    pipeline = BronzePipeline(store, settings, CONTRACTS, dry_run=dry_run)
    return pipeline, settings, store


def _snapshot(store: InMemoryObjectStore, bucket: str) -> dict[str, bytes]:
    return {key: store.get_bytes(bucket, key) for key in store.list_keys(bucket)}


def _payload(store: InMemoryObjectStore, key: str | None) -> dict[str, Any]:
    assert key is not None
    payload: dict[str, Any] = json.loads(store.get_bytes(BRONZE_BUCKET, key))
    return payload


def test_full_run_promotes_every_valid_batch_and_leaves_raw_untouched(
    tmp_path: Path,
) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_dimensions(store, settings)
    seed_raw(store, settings, "sales", [sales_row(), sales_row(sale_id="S000000002")])
    raw_before = _snapshot(store, RAW_BUCKET)

    result = pipeline.run()

    assert len(result.records) == 5
    for record in result.records:
        assert record.outcome == "bronzed"
        assert record.quality_status == "passed"
        assert record.promoted is True
        assert record.expectations_failed == 0
        assert record.part_keys
        for key in record.part_keys:
            assert store.object_exists(BRONZE_BUCKET, key)
            frame = pd.read_parquet(io.BytesIO(store.get_bytes(BRONZE_BUCKET, key)))
            assert PROVENANCE <= set(frame.columns)
            assert frame["source"].unique().tolist() == [record.source]
            assert frame["ingestion_run_id"].unique().tolist() == [result.run_id]
        quality = _payload(store, record.quality_key)
        assert quality["status"] == "passed"
        assert quality["records_checked"] == record.rows_in
        assert quality["contract"]["id"] == record.source
        marker = _payload(store, record.promoted_key)
        assert marker["source"] == record.source
        assert marker["quality"]["status"] == "passed"

    assert _snapshot(store, RAW_BUCKET) == raw_before
    assert result.counts["bronzed"] == 5
    assert result.manifest["quality"] == {"passed": 5, "failed": 0}
    assert result.manifest["promoted"] == 5
    assert result.manifest["batch_count"] == 5
    assert result.manifest_path is not None and result.manifest_path.is_file()
    assert store.object_exists(
        BRONZE_BUCKET, f"{settings.bronze_manifest_prefix}/{result.run_id}.json"
    )


def test_batch_id_follows_the_raw_object_stem(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(store, settings, "regions", [region_row()], tag="cafef00ddeadbeef")
    result = pipeline.run(sources=["regions"])
    record = result.records[0]
    assert record.raw_key == raw_key("regions", "cafef00ddeadbeef")
    assert record.batch == "regions__cafef00ddeadbeef"


def test_failed_quality_batch_keeps_parts_and_report_without_promotion(
    tmp_path: Path,
) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(
        store,
        settings,
        "sales",
        [sales_row(), sales_row(sale_id="S2", quantity="-2")],
    )
    raw_before = _snapshot(store, RAW_BUCKET)

    result = pipeline.run(sources=["sales"])
    record = result.records[0]

    assert record.outcome == "failed"
    assert record.quality_status == "failed"
    assert record.promoted is False
    assert record.promoted_key is None
    assert record.rejected_key is None
    assert record.expectations_failed >= 1
    assert record.part_keys
    for key in record.part_keys:
        assert store.object_exists(BRONZE_BUCKET, key)
    quality = _payload(store, record.quality_key)
    assert quality["status"] == "failed"
    assert quality["failed_expectations"]
    assert any("quantity >= 1" in entry["declared"] for entry in quality["failed_expectations"])
    assert quality["skipped_expectations"]
    assert store.list_keys(BRONZE_BUCKET, f"{settings.promoted_prefix}/sales/") == []
    assert _snapshot(store, RAW_BUCKET) == raw_before
    assert result.counts["failed"] == 1
    assert result.manifest["quality"] == {"passed": 0, "failed": 1}


def test_conversion_rejected_rows_are_persisted_and_block_promotion(
    tmp_path: Path,
) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(
        store,
        settings,
        "sales",
        [sales_row(), sales_row(sale_id="S2", unit_price_mad="not-a-price")],
    )

    result = pipeline.run(sources=["sales"])
    record = result.records[0]

    assert record.outcome == "failed"
    assert record.rows_rejected == 1
    assert record.rejected_key is not None
    rejected = _payload(store, record.rejected_key)
    assert rejected["rows_in"] == 2
    assert rejected["rows_rejected"] == 1
    assert rejected["issue_counts"] == {"invalid_number": 1}
    assert rejected["rejected_rows"][0]["unit_price_mad"] == "not-a-price"
    quality = _payload(store, record.quality_key)
    assert quality["status"] == "failed"
    assert "rejected" in quality["reason"]
    assert store.list_keys(BRONZE_BUCKET, f"{settings.promoted_prefix}/") == []


def test_fatal_missing_column_writes_only_the_quality_report(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    contract = CONTRACTS["sales"]
    header = [c.name for c in contract.columns if c.name != "unit_price_mad"]
    payload = (",".join(header) + "\n" + ",".join("x" for _ in header) + "\n").encode("utf-8")
    store.put_bytes(RAW_BUCKET, raw_key("sales"), payload)

    result = pipeline.run(sources=["sales"])
    record = result.records[0]

    assert record.outcome == "failed"
    assert record.quality_status == "failed"
    assert record.reason is not None
    assert "missing required column(s): unit_price_mad" in record.reason
    assert record.part_keys == ()
    assert store.list_keys(BRONZE_BUCKET, "bronze/sales/") == []
    quality = _payload(store, record.quality_key)
    assert quality["status"] == "failed"
    assert quality["expectations_total"] == 0
    assert store.list_keys(BRONZE_BUCKET, f"{settings.promoted_prefix}/") == []


def test_second_run_is_idempotent(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(store, settings, "channels", [channel_row()])

    first = pipeline.run(sources=["channels"])
    second = pipeline.run(sources=["channels"])

    assert first.records[0].outcome == "bronzed"
    assert second.records[0].outcome == "already_bronzed"
    assert second.records[0].part_keys == first.records[0].part_keys
    assert second.records[0].promoted is True
    assert second.records[0].quality_status == "passed"


def test_dry_run_plans_without_creating_the_bronze_bucket(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path, dry_run=True)
    seed_raw(store, settings, "regions", [region_row()])

    result = pipeline.run(sources=["regions"])
    record = result.records[0]

    assert record.outcome == "planned"
    assert record.quality_status is None
    assert record.quality_key is None
    assert record.promoted_key is None
    assert record.part_keys
    assert record.rows_kept == 1
    assert not store.bucket_exists(BRONZE_BUCKET)
    assert result.manifest_path is None
    assert result.dry_run is True
    assert result.manifest["dry_run"] is True


def test_unknown_source_is_a_usage_error(tmp_path: Path) -> None:
    pipeline, _settings, _store = _pipeline(tmp_path)
    with pytest.raises(UsageError, match="unknown source"):
        pipeline.run(sources=["nope"])


def test_explicit_source_without_raw_objects_is_a_usage_error(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(store, settings, "regions", [region_row()])
    with pytest.raises(UsageError, match="no raw objects for source"):
        pipeline.run(sources=["sales"])


def test_unknown_batch_key_is_a_usage_error(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(store, settings, "regions", [region_row()])
    with pytest.raises(UsageError, match="raw object"):
        pipeline.run(only_keys=["raw/regions/regions__ffffffffffffffff.csv"])


def test_manifest_records_every_outcome_bucket(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(store, settings, "regions", [region_row()])

    result: BronzeRunResult = pipeline.run(sources=["regions"])

    counts = result.counts
    assert set(counts) == {"bronzed", "already_bronzed", "failed", "planned"}
    assert counts["bronzed"] == 1
    assert set(result.manifest) >= {
        "run_id",
        "started_at",
        "finished_at",
        "dry_run",
        "batch_count",
        "counts",
        "quality",
        "promoted",
        "records",
    }
    assert result.records[0].timestamp
    assert result.records[0].contract_id == "regions"
    assert result.records[0].contract_version == CONTRACTS["regions"].version
    assert result.records[0].input_bytes == len(csv_bytes("regions", [region_row()]))
    assert DEFAULT_TAG in result.records[0].raw_key


def test_valid_batch_is_promoted_even_when_sibling_fails(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    good_key = seed_raw(store, settings, "sales", [sales_row()], tag="good000000000001")
    bad_key = seed_raw(
        store, settings, "sales", [sales_row(sale_id="S2", quantity="-5")], tag="bad000000000002"
    )

    result = pipeline.run(sources=["sales"])

    by_key = {record.raw_key: record for record in result.records}
    assert good_key in by_key
    assert bad_key in by_key
    assert by_key[good_key].promoted is True
    assert by_key[good_key].outcome == "bronzed"
    assert by_key[good_key].quality_status == "passed"
    assert by_key[bad_key].promoted is False
    assert by_key[bad_key].outcome == "failed"
    assert by_key[bad_key].quality_status == "failed"
    promoted = [record for record in result.records if record.raw_key == good_key]
    assert promoted and promoted[0].promoted
