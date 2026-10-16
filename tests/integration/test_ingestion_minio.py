"""Integration tests against a live MinIO instance (skipped when unavailable)."""

from __future__ import annotations

from pathlib import Path

import pytest

from mri.ingestion.config import Settings
from mri.ingestion.contracts import load_all_contracts
from mri.ingestion.pipeline import IngestionPipeline
from mri.ingestion.storage import MinioObjectStore

TEST_BUCKET = "retail-ingestion-it"
VALID = "region_id,region_name,demand_weight\nR01,Alpha,0.5\n"


def _settings(tmp_path: Path) -> Settings:
    base = Settings.from_env()
    return Settings(
        minio_endpoint=base.minio_endpoint,
        minio_access_key=base.minio_access_key,
        minio_secret_key=base.minio_secret_key,
        minio_secure=base.minio_secure,
        bucket_raw=TEST_BUCKET,
        bucket_bronze=base.bucket_bronze,
        contracts_dir=base.contracts_dir,
        manifest_dir=tmp_path / "manifests",
    )


def _cleanup_bucket(settings: Settings) -> None:
    from minio import Minio

    if not settings.minio_access_key:
        return
    client = Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    if not client.bucket_exists(TEST_BUCKET):
        return
    for obj in client.list_objects(TEST_BUCKET, recursive=True):
        client.remove_object(TEST_BUCKET, obj.object_name)
    client.remove_bucket(TEST_BUCKET)


def test_minio_roundtrip(tmp_path: Path) -> None:
    settings = _settings(tmp_path)
    if not settings.minio_access_key:
        pytest.skip("MinIO credentials not configured")
    store = MinioObjectStore(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    try:
        store.bucket_exists(TEST_BUCKET)
    except Exception as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"MinIO not reachable: {exc}")

    try:
        pipeline = IngestionPipeline(store, settings, load_all_contracts())
        path = tmp_path / "regions.csv"
        path.write_text(VALID, encoding="utf-8")

        first = pipeline.ingest_path(path, source="regions")
        assert first.records[0].outcome == "ingested"
        key = first.records[0].object_key
        assert key is not None
        assert store.get_bytes(TEST_BUCKET, key) == path.read_bytes()

        second = pipeline.ingest_path(path, source="regions")
        assert second.records[0].outcome == "already_ingested"

        manifest_uploaded = store.object_exists(TEST_BUCKET, f"_manifests/{first.run_id}.json")
        assert manifest_uploaded
    finally:
        _cleanup_bucket(settings)

    assert not store.bucket_exists(TEST_BUCKET)
