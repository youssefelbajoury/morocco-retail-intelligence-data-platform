"""Integration test: external ingestion against live MinIO (skipped when unavailable).

HTTP is stubbed (no live API calls); only the object store is real, so this
proves the ``raw_external/`` write path, provenance sidecars and idempotent
reruns against the containerized MinIO from PHASE 03.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

from mri.ingestion.clients.base import FetchOptions, FetchOutcome, FetchRequest
from mri.ingestion.clients.fx_client import SOURCE as FX_SOURCE
from mri.ingestion.config import Settings
from mri.ingestion.external import ExternalPipeline, request_hash
from mri.ingestion.storage import MinioObjectStore

TEST_BUCKET = "retail-external-it"
DAY = date(2026, 10, 7)
RAW = b'[{"date":"2026-10-07","base":"EUR","quote":"MAD","rate":11.2059}]'


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
        external_user_agent="MoroccoRetailIntelligence-Tests/0.1.0 (tests@example.invalid)",
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


class StubFxClient:
    """Offline Frankfurter stand-in for the MinIO round-trip."""

    sources: tuple[str, ...] = (FX_SOURCE,)

    def build_requests(self, options: FetchOptions) -> list[FetchRequest]:
        return [
            FetchRequest(
                source=FX_SOURCE,
                data_type="fx_rates",
                url="https://fx.test/v2/rates",
                params={"base": "EUR", "quotes": "MAD,USD,GBP"},
                context={"provider": "Frankfurter (ECB reference rates)"},
            )
        ]

    def fetch(self, request: FetchRequest) -> FetchOutcome:
        return FetchOutcome(
            request=request,
            payload=[],
            raw=RAW,
            status_code=200,
            attempts=1,
            elapsed_ms=3,
        )


def test_external_minio_roundtrip(tmp_path: Path) -> None:
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
        client = StubFxClient()
        pipeline = ExternalPipeline(store, settings, [client], today=DAY)
        options = FetchOptions(sources=(FX_SOURCE,))

        first = pipeline.run(options)
        record = first.records[0]
        assert record.outcome == "fetched"
        assert record.object_key is not None
        assert record.metadata_key is not None
        assert store.get_bytes(TEST_BUCKET, record.object_key) == RAW

        metadata = json.loads(store.get_bytes(TEST_BUCKET, record.metadata_key))
        assert metadata["source"] == FX_SOURCE
        assert metadata["request_hash"] == record.request_hash
        assert metadata["retrieved_date"] == "2026-10-07"
        digest = request_hash(client.build_requests(options)[0])
        assert record.request_hash == digest
        assert f"request_hash={digest}" in record.object_key

        assert store.object_exists(TEST_BUCKET, f"_manifests/{first.run_id}.json")
        assert first.manifest_path is not None and first.manifest_path.is_file()

        second = pipeline.run(options)
        assert second.records[0].outcome == "already_fetched"
        assert client.build_requests(options)  # stub is deterministic
    finally:
        _cleanup_bucket(settings)

    assert not store.bucket_exists(TEST_BUCKET)
