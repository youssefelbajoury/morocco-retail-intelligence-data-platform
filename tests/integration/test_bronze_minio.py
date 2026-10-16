"""Integration tests for the Bronze pipeline against a live MinIO instance.

Skipped automatically when MinIO is unreachable or credentials are absent.
Raw objects are compared byte-for-byte before/after to prove Bronze never
mutates Raw, and promotion markers are asserted on both pass and failure.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path

import pandas as pd
import pytest

from mri.ingestion.bronze import BronzePipeline
from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract, load_all_contracts
from mri.ingestion.storage import MinioObjectStore

RAW_BUCKET = "retail-bronze-it-raw"
BRONZE_BUCKET = "retail-bronze-it-bronze"
TAG = "integration00001"


def _settings(tmp_path: Path) -> Settings:
    base = Settings.from_env()
    return Settings(
        minio_endpoint=base.minio_endpoint,
        minio_access_key=base.minio_access_key,
        minio_secret_key=base.minio_secret_key,
        minio_secure=base.minio_secure,
        bucket_raw=RAW_BUCKET,
        bucket_bronze=BRONZE_BUCKET,
        contracts_dir=base.contracts_dir,
        manifest_dir=tmp_path / "manifests",
        bronze_manifest_dir=tmp_path / "bronze-manifests",
    )


def _cleanup(settings: Settings) -> None:
    from minio import Minio

    if not settings.minio_access_key:
        return
    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    for bucket in (RAW_BUCKET, BRONZE_BUCKET):
        if not client.bucket_exists(bucket):
            continue
        for obj in client.list_objects(bucket, recursive=True):
            client.remove_object(bucket, obj.object_name)
        client.remove_bucket(bucket)


def _live_store(settings: Settings) -> MinioObjectStore:
    if not settings.minio_access_key:
        pytest.skip("MinIO credentials not configured")
    store = MinioObjectStore(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    try:
        store.bucket_exists(RAW_BUCKET)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"MinIO not reachable: {exc}")
    return store


def _csv_bytes(contract: Contract, rows: list[dict[str, str]]) -> bytes:
    header = [column.name for column in contract.columns]
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=header, extrasaction="ignore", restval="")
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue().encode("utf-8")


def _seed(
    store: MinioObjectStore, contracts: dict[str, Contract], source: str, rows: list[dict[str, str]]
) -> str:
    key = f"raw/{source}/{source}__{TAG}.csv"
    store.ensure_bucket(RAW_BUCKET)
    store.put_bytes(RAW_BUCKET, key, _csv_bytes(contracts[source], rows))
    return key


REGION_ROW = {"region_id": "R01", "region_name": "Casablanca-Settat", "demand_weight": "0.25"}
CHANNEL_ROW = {
    "channel_id": "CH01",
    "channel_name": "Flagship Store",
    "channel_type": "physical",
    "demand_weight": "0.5",
}
STORE_ROW = {
    "store_id": "ST01",
    "store_name": "Gueliz Flagship",
    "city": "Marrakech",
    "region_id": "R01",
    "store_format": "hypermarket",
    "size_sqm": "3500",
    "open_date": "2019-06-01",
    "is_active": "true",
}
SALES_ROW = {
    "sale_id": "S000000001",
    "transaction_id": "T00000001",
    "sale_date": "2024-06-15",
    "hour_of_day": "12",
    "store_id": "ST01",
    "channel_id": "CH01",
    "customer_id": "CU0001",
    "product_id": "P000001",
    "promotion_id": "",
    "quantity": "2",
    "unit_price_mad": "25.50",
    "discount_pct": "0.10",
    "net_amount_mad": "45.90",
    "cost_amount_mad": "30.00",
    "margin_mad": "15.90",
}


def test_bronze_roundtrip_promotes_valid_batches_and_leaves_raw_untouched(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = _live_store(settings)
    contracts = load_all_contracts()
    try:
        key_region = _seed(store, contracts, "regions", [REGION_ROW])
        _seed(store, contracts, "stores", [STORE_ROW])
        _seed(store, contracts, "channels", [CHANNEL_ROW])
        raw_before = {key: store.get_bytes(RAW_BUCKET, key) for key in store.list_keys(RAW_BUCKET)}

        pipeline = BronzePipeline(store, settings, contracts)
        result = pipeline.run(sources=["regions", "stores", "channels"])

        assert len(result.records) == 3
        for record in result.records:
            assert record.outcome == "bronzed"
            assert record.quality_status == "passed"
            assert record.promoted is True
            assert record.part_keys
            blob = store.get_bytes(BRONZE_BUCKET, record.part_keys[0])
            frame = pd.read_parquet(io.BytesIO(blob))
            assert "source" in frame.columns
            assert "source_hash" in frame.columns
            assert record.quality_key is not None
            assert store.object_exists(BRONZE_BUCKET, record.quality_key)
            assert record.promoted_key is not None
            assert store.object_exists(BRONZE_BUCKET, record.promoted_key)

        assert store.get_bytes(RAW_BUCKET, key_region) == raw_before[key_region]
        for key, blob in raw_before.items():
            assert store.get_bytes(RAW_BUCKET, key) == blob

        second = pipeline.run(sources=["regions", "stores", "channels"])
        assert {record.outcome for record in second.records} == {"already_bronzed"}
    finally:
        _cleanup(settings)


def test_failed_quality_batch_is_not_promoted_but_keeps_report_and_parts(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = _live_store(settings)
    contracts = load_all_contracts()
    try:
        _seed(store, contracts, "regions", [REGION_ROW])
        _seed(store, contracts, "stores", [STORE_ROW])
        _seed(store, contracts, "channels", [CHANNEL_ROW])
        bad_sales = dict(SALES_ROW, quantity="-5")
        _seed(store, contracts, "sales", [bad_sales])

        pipeline = BronzePipeline(store, settings, contracts)
        result = pipeline.run(sources=["sales"])
        record = result.records[0]

        assert record.outcome == "failed"
        assert record.quality_status == "failed"
        assert record.promoted is False
        assert record.promoted_key is None
        assert record.quality_key is not None
        assert store.object_exists(BRONZE_BUCKET, record.quality_key)
        assert record.part_keys
        assert store.object_exists(BRONZE_BUCKET, record.part_keys[0])
        assert store.list_keys(BRONZE_BUCKET, "_promoted/") == []
    finally:
        _cleanup(settings)
