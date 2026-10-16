"""Integration tests for external Bronze against a live MinIO instance.

Skipped automatically when MinIO is unreachable or credentials are absent.
Verbatim ``raw_external/`` response + metadata objects are seeded in an
isolated bucket pair, bronzed through the real Great Expectations gate, and
compared byte-for-byte afterwards to prove the raw layer is never mutated.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from mri.ingestion.bronze import BronzePipeline
from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract, load_all_contracts
from mri.ingestion.storage import MinioObjectStore

RAW_BUCKET = "retail-extbronze-it-raw"
BRONZE_BUCKET = "retail-extbronze-it-bronze"
DAY = "2026-10-07"

HISTORY = "open_meteo_history"
FX = "frankfurter_fx"
PRODUCTS = "openfoodfacts_products"

LOCATION = {
    "region_id": "R01",
    "region_name": "Casablanca-Settat",
    "city": "Casablanca",
    "latitude": 33.5731,
    "longitude": -7.5898,
}

REGION_ROW = {"region_id": "R01", "region_name": "Casablanca-Settat", "demand_weight": "0.25"}


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


def _seed_region(store: MinioObjectStore, contracts: dict[str, Contract]) -> str:
    contract = contracts["regions"]
    header = [column.name for column in contract.columns]
    csv_bytes = (",".join(header) + "\n" + ",".join(REGION_ROW[h] for h in header) + "\n").encode(
        "utf-8"
    )
    key = "raw/regions/regions__integration00001.csv"
    store.ensure_bucket(RAW_BUCKET)
    store.put_bytes(RAW_BUCKET, key, csv_bytes)
    return key


def _seed_external(
    store: MinioObjectStore, source: str, payload: Any, metadata: Any, digest: str
) -> str:
    base = f"raw_external/source={source}/retrieved_date={DAY}/request_hash={digest}"
    store.ensure_bucket(RAW_BUCKET)
    store.put_bytes(RAW_BUCKET, f"{base}/response.json", json.dumps(payload).encode("utf-8"))
    store.put_bytes(RAW_BUCKET, f"{base}/metadata.json", json.dumps(metadata).encode("utf-8"))
    return f"{base}/response.json"


def _weather_payload() -> dict[str, Any]:
    return {
        "latitude": 33.57,
        "longitude": -7.59,
        "timezone": "Africa/Casablanca",
        "daily": {
            "time": ["2026-09-27", "2026-09-28"],
            "weather_code": [3, 1],
            "temperature_2m_max": [28.4, 29.1],
            "temperature_2m_min": [20.8, 21.2],
            "precipitation_sum": [0.0, 0.4],
            "precipitation_hours": [0.0, 2.0],
            "wind_speed_10m_max": [18.5, 21.0],
        },
    }


def _weather_metadata() -> dict[str, Any]:
    return {
        "kind": "external_api_response",
        "source": HISTORY,
        "data_type": "historical",
        "retrieved_at": f"{DAY}T12:00:00+00:00",
        "retrieved_date": DAY,
        "context": {"timezone": "Africa/Casablanca", "location": dict(LOCATION)},
    }


def _fx_metadata() -> dict[str, Any]:
    return {
        "kind": "external_api_response",
        "source": FX,
        "data_type": "fx_rates",
        "retrieved_at": f"{DAY}T12:00:00+00:00",
        "retrieved_date": DAY,
        "context": {"base": "EUR"},
    }


def _product_payload(code: str, *, found: bool = True) -> dict[str, Any]:
    if not found:
        return {"code": code, "status": "failure", "product": {}}
    return {
        "code": code,
        "status": "success",
        "product": {
            "code": code,
            "product_name": "Nutella",
            "nutriscore_grade": "e",
            "nova_group": 4,
            "nutriments": {"energy-kcal_100g": 539, "fat_100g": 30.9, "salt_100g": 0.107},
        },
    }


def _product_metadata(code: str, *, found: bool = True) -> dict[str, Any]:
    return {
        "kind": "external_api_response",
        "source": PRODUCTS,
        "data_type": "product_catalog",
        "retrieved_at": f"{DAY}T12:00:00+00:00",
        "retrieved_date": DAY,
        "result": "found" if found else "not_found",
        "mapping": {
            "external_product_id": code,
            "internal_product_id": None,
            "source": PRODUCTS,
            "mapping_status": "NEW" if found else "UNMATCHED",
        },
    }


def test_external_bronze_roundtrip_promotes_valid_batches_and_leaves_raw_untouched(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = _live_store(settings)
    contracts = load_all_contracts()
    try:
        _seed_region(store, contracts)
        keys = {
            _seed_external(
                store,
                HISTORY,
                _weather_payload(),
                _weather_metadata(),
                "a" * 64,
            ),
            _seed_external(
                store,
                FX,
                [
                    {"date": "2026-10-06", "base": "EUR", "quote": "MAD", "rate": 11.2003},
                    {"date": "2026-10-06", "base": "EUR", "quote": "USD", "rate": 1.1236},
                ],
                _fx_metadata(),
                "b" * 64,
            ),
            _seed_external(
                store,
                PRODUCTS,
                _product_payload("3017620422003"),
                _product_metadata("3017620422003"),
                "c" * 64,
            ),
            _seed_external(
                store,
                PRODUCTS,
                _product_payload("5000112637922", found=False),
                _product_metadata("5000112637922", found=False),
                "d" * 64,
            ),
        }
        raw_before = {key: store.get_bytes(RAW_BUCKET, key) for key in store.list_keys(RAW_BUCKET)}

        pipeline = BronzePipeline(store, settings, contracts)
        result = pipeline.run(sources=[HISTORY, FX, PRODUCTS])

        assert len(result.records) == 4
        assert result.counts["bronzed"] == 4
        for record in result.records:
            assert record.outcome == "bronzed"
            assert record.quality_status == "passed"
            assert record.promoted is True
            assert record.rows_rejected == 0
            assert record.batch.startswith(f"{record.source}__")
            assert record.part_keys
            frame = pd.read_parquet(io.BytesIO(store.get_bytes(BRONZE_BUCKET, record.part_keys[0])))
            assert "source" in frame.columns
            assert "source_hash" in frame.columns
            assert len(frame) == record.rows_kept
            assert record.quality_key is not None
            assert store.object_exists(BRONZE_BUCKET, record.quality_key)
            assert record.promoted_key is not None
            assert store.object_exists(BRONZE_BUCKET, record.promoted_key)
        assert {record.raw_key for record in result.records} == keys

        for key, blob in raw_before.items():
            assert store.get_bytes(RAW_BUCKET, key) == blob

        second = pipeline.run(sources=[HISTORY, FX, PRODUCTS])
        assert {record.outcome for record in second.records} == {"already_bronzed"}
    finally:
        _cleanup(settings)


def test_external_quality_failure_is_not_promoted_but_keeps_report_and_parts(
    tmp_path: Path,
) -> None:
    settings = _settings(tmp_path)
    store = _live_store(settings)
    contracts = load_all_contracts()
    try:
        _seed_external(
            store,
            FX,
            [{"date": "2026-10-06", "base": "EUR", "quote": "MAD", "rate": -1.0}],
            _fx_metadata(),
            "e" * 64,
        )

        pipeline = BronzePipeline(store, settings, contracts)
        result = pipeline.run(sources=[FX])
        record = result.records[0]

        assert record.outcome == "failed"
        assert record.quality_status == "failed"
        assert record.promoted is False
        assert record.promoted_key is None
        assert record.expectations_failed >= 1
        assert record.quality_key is not None
        assert store.object_exists(BRONZE_BUCKET, record.quality_key)
        assert record.part_keys
        assert store.object_exists(BRONZE_BUCKET, record.part_keys[0])
        assert store.list_keys(BRONZE_BUCKET, "_promoted/") == []
    finally:
        _cleanup(settings)
