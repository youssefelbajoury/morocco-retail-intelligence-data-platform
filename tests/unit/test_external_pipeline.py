"""Unit tests for the external ingestion orchestrator (storage + provenance).

The pipeline is exercised with stub clients and an in-memory object store:
dry-run performs no HTTP and no writes, fetched responses land verbatim under
the documented ``raw_external/`` keys with a provenance sidecar, same-day
reruns are idempotent, and failures become manifest records without aborting
the rest of the run.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from mri.ingestion.clients.base import FetchOptions, FetchOutcome, FetchRequest
from mri.ingestion.clients.fx_client import SOURCE as FX_SOURCE
from mri.ingestion.clients.product_catalog_client import SOURCE as PRODUCTS_SOURCE
from mri.ingestion.clients.weather_client import SOURCE_HISTORY
from mri.ingestion.config import Settings
from mri.ingestion.exceptions import ApiRetryError, UsageError
from mri.ingestion.external import (
    ExternalPipeline,
    load_internal_barcode_index,
    read_barcodes,
    request_hash,
    resolve_sources,
    response_keys,
)
from mri.ingestion.storage import InMemoryObjectStore

DAY = date(2026, 10, 7)
DAY_ISO = "2026-10-07"


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    defaults: dict[str, Any] = {
        "minio_endpoint": "localhost:9000",
        "minio_access_key": "test",
        "minio_secret_key": "test",
        "minio_secure": False,
        "bucket_raw": "retail-raw",
        "bucket_bronze": "retail-bronze",
        "contracts_dir": tmp_path / "contracts",
        "manifest_dir": tmp_path / "reports" / "ingestion",
        "external_user_agent": "MoroccoRetailIntelligence-Tests/0.1.0 (tests@example.invalid)",
    }
    defaults.update(overrides)
    return Settings(**defaults)


def fx_request(date_value: str = "2026-10-06") -> FetchRequest:
    return FetchRequest(
        source=FX_SOURCE,
        data_type="fx_rates",
        url="https://fx.test/v2/rates",
        params={"base": "EUR", "quotes": "MAD,USD,GBP", "date": date_value},
        context={"provider": "Frankfurter (ECB reference rates)", "api_key_required": False},
    )


def product_request(barcode: str = "3017620422003") -> FetchRequest:
    return FetchRequest(
        source=PRODUCTS_SOURCE,
        data_type="product_catalog",
        url=f"https://off.test/api/v3/product/{barcode}.json",
        context={"provider": "Open Food Facts", "barcode": barcode, "api_key_required": False},
    )


def outcome_for(
    request: FetchRequest,
    *,
    raw: bytes = b'{"rates": []}',
    found: bool | None = None,
    mapping: dict[str, Any] | None = None,
) -> FetchOutcome:
    return FetchOutcome(
        request=request,
        payload={},
        raw=raw,
        status_code=200,
        attempts=1,
        elapsed_ms=12,
        found=found,
        mapping=mapping,
    )


class StubClient:
    """A fake external client: preset requests, scripted fetch results."""

    def __init__(
        self,
        requests: list[FetchRequest],
        result: Any = None,
        sources: tuple[str, ...] = (FX_SOURCE,),
    ) -> None:
        self.sources = sources
        self._requests = requests
        self._result = result
        self.fetch_calls = 0

    def build_requests(self, options: FetchOptions) -> list[FetchRequest]:
        return list(self._requests)

    def fetch(self, request: FetchRequest) -> FetchOutcome:
        self.fetch_calls += 1
        if isinstance(self._result, Exception):
            raise self._result
        if callable(self._result):
            scripted: FetchOutcome = self._result(request)
            return scripted
        return outcome_for(request)


def run_pipeline(
    tmp_path: Path,
    client: StubClient,
    *,
    dry_run: bool = False,
    store: InMemoryObjectStore | None = None,
) -> tuple[Any, InMemoryObjectStore]:
    settings = make_settings(tmp_path)
    object_store = store if store is not None else InMemoryObjectStore()
    pipeline = ExternalPipeline(
        None if dry_run else object_store, settings, [client], dry_run=dry_run, today=DAY
    )
    result = pipeline.run(FetchOptions(sources=(FX_SOURCE,)))
    return result, object_store


# --------------------------------------------------------------------------
# source resolution / helpers
# --------------------------------------------------------------------------


def test_resolve_sources_expands_aliases_in_stable_order() -> None:
    assert resolve_sources([]) == (
        SOURCE_HISTORY,
        "open_meteo_forecast",
        FX_SOURCE,
        PRODUCTS_SOURCE,
    )
    assert resolve_sources(["fx", "weather-history"]) == (SOURCE_HISTORY, FX_SOURCE)
    assert resolve_sources(["products", "fx"]) == (FX_SOURCE, PRODUCTS_SOURCE)


def test_resolve_sources_rejects_unknown() -> None:
    with pytest.raises(UsageError, match="unknown --source"):
        resolve_sources(["crypto"])


def test_request_hash_ignores_param_order_but_not_values() -> None:
    first = FetchRequest(source=FX_SOURCE, data_type="fx", url="u", params={"a": "1", "b": "2"})
    second = FetchRequest(source=FX_SOURCE, data_type="fx", url="u", params={"b": "2", "a": "1"})
    third = FetchRequest(source=FX_SOURCE, data_type="fx", url="u", params={"a": "1", "b": "3"})
    assert request_hash(first) == request_hash(second)
    assert request_hash(first) != request_hash(third)
    assert len(request_hash(first)) == 64


def test_response_keys_follow_the_documented_layout() -> None:
    response_key, metadata_key = response_keys("raw_external", FX_SOURCE, DAY_ISO, "abc")
    assert response_key == (
        f"raw_external/source={FX_SOURCE}/retrieved_date={DAY_ISO}/request_hash=abc/response.json"
    )
    assert metadata_key == response_key.replace("response.json", "metadata.json")


def test_read_barcodes_supports_comments(tmp_path: Path) -> None:
    path = tmp_path / "barcodes.txt"
    path.write_text("# products to enrich\n3017620422003\n\n5000112637922\n", encoding="utf-8")
    assert read_barcodes(path) == ("3017620422003", "5000112637922")
    empty = tmp_path / "empty.txt"
    empty.write_text("# only comments\n", encoding="utf-8")
    with pytest.raises(UsageError, match="no barcodes"):
        read_barcodes(empty)
    with pytest.raises(UsageError, match="cannot read barcode file"):
        read_barcodes(tmp_path / "missing.txt")


def test_internal_index_requires_expected_columns(tmp_path: Path) -> None:
    assert load_internal_barcode_index(None) == {}
    good = tmp_path / "products.csv"
    good.write_text("barcode,product_id\n3017620422003,P000001\n", encoding="utf-8")
    assert load_internal_barcode_index(good) == {"3017620422003": "P000001"}
    bad = tmp_path / "bad.csv"
    bad.write_text("sku,id\n1,2\n", encoding="utf-8")
    with pytest.raises(UsageError, match="barcode"):
        load_internal_barcode_index(bad)


# --------------------------------------------------------------------------
# dry-run
# --------------------------------------------------------------------------


def test_dry_run_plans_without_http_or_writes(tmp_path: Path) -> None:
    def explode(request: FetchRequest) -> FetchOutcome:
        raise AssertionError("dry-run must not fetch")

    client = StubClient([fx_request()], explode)
    settings = make_settings(tmp_path)
    store = InMemoryObjectStore()
    pipeline = ExternalPipeline(store, settings, [client], dry_run=True, today=DAY)

    result = pipeline.run(FetchOptions(sources=(FX_SOURCE,)))

    assert result.dry_run is True
    assert [record.outcome for record in result.records] == ["planned"]
    assert client.fetch_calls == 0
    assert store.list_keys("retail-raw") == []
    assert result.manifest_path is None
    assert result.manifest["counts"]["planned"] == 1
    record = result.records[0]
    assert record.object_key is not None and record.object_key.endswith("response.json")
    assert record.object_key.startswith("raw_external/source=frankfurter_fx/")


# --------------------------------------------------------------------------
# fetching + provenance
# --------------------------------------------------------------------------


def test_fetch_writes_verbatim_response_and_provenance(tmp_path: Path) -> None:
    raw = b'[{"date":"2026-10-06","base":"EUR","quote":"MAD","rate":11.2}]'
    request = fx_request()
    client = StubClient([request], lambda req: outcome_for(req, raw=raw))

    result, store = run_pipeline(tmp_path, client)

    assert [record.outcome for record in result.records] == ["fetched"]
    record = result.records[0]
    digest = request_hash(request)
    assert record.object_key == (
        f"raw_external/source={FX_SOURCE}/retrieved_date={DAY_ISO}/"
        f"request_hash={digest}/response.json"
    )
    assert record.metadata_key == record.object_key.replace("response.json", "metadata.json")
    assert store.get_bytes("retail-raw", record.object_key or "") == raw
    assert record.sha256 == hashlib.sha256(raw).hexdigest()
    assert record.size_bytes == len(raw)
    assert record.http_status == 200

    metadata = json.loads(store.get_bytes("retail-raw", record.metadata_key or ""))
    assert metadata["kind"] == "external_api_response"
    assert metadata["schema_version"] == "1.0.0"
    assert metadata["source"] == FX_SOURCE
    assert metadata["data_type"] == "fx_rates"
    assert metadata["request_hash"] == digest
    assert metadata["retrieved_date"] == DAY_ISO
    assert metadata["run_id"] == result.run_id
    assert metadata["endpoint"] == request.url
    assert metadata["request"]["params"]["quotes"] == "MAD,USD,GBP"
    assert metadata["http"]["status"] == 200
    assert metadata["http"]["attempts"] == 1
    assert metadata["response"]["sha256"] == hashlib.sha256(raw).hexdigest()
    assert metadata["auth"] == {
        "api_key_required": False,
        "credentials_sent": False,
        "user_agent": "MoroccoRetailIntelligence-Tests/0.1.0 (tests@example.invalid)",
    }
    assert metadata["client"]["library"] == "httpx"
    assert metadata["context"]["provider"].startswith("Frankfurter")


def test_same_day_rerun_is_idempotent(tmp_path: Path) -> None:
    client = StubClient([fx_request()])
    first, store = run_pipeline(tmp_path, client)
    second, _ = run_pipeline(tmp_path, client, store=store)

    assert [record.outcome for record in first.records] == ["fetched"]
    assert [record.outcome for record in second.records] == ["already_fetched"]
    assert client.fetch_calls == 1
    assert len(store.list_keys("retail-raw", "raw_external/")) == 2  # response + metadata only


def test_product_mapping_is_persisted_in_metadata(tmp_path: Path) -> None:
    request = product_request()
    mapping = {
        "external_product_id": "3017620422003",
        "internal_product_id": None,
        "source": PRODUCTS_SOURCE,
        "mapping_status": "NEW",
    }
    client = StubClient(
        [request],
        lambda req: outcome_for(req, raw=b'{"code":"3017620422003"}', found=True, mapping=mapping),
        sources=(PRODUCTS_SOURCE,),
    )
    settings = make_settings(tmp_path)
    store = InMemoryObjectStore()
    pipeline = ExternalPipeline(store, settings, [client], today=DAY)

    result = pipeline.run(FetchOptions(sources=(PRODUCTS_SOURCE,)))

    record = result.records[0]
    metadata = json.loads(store.get_bytes("retail-raw", record.metadata_key or ""))
    assert metadata["result"] == "found"
    assert metadata["mapping"] == mapping


# --------------------------------------------------------------------------
# failures
# --------------------------------------------------------------------------


def test_failure_is_recorded_and_the_run_continues(tmp_path: Path) -> None:
    first = fx_request("2026-10-05")
    second = fx_request("2026-10-06")

    def scripted(request: FetchRequest) -> FetchOutcome:
        if request.params["date"] == "2026-10-05":
            raise ApiRetryError("timeout after 4 attempt(s)", attempts=4, reason="timeout")
        return outcome_for(request)

    client = StubClient([first, second], scripted)
    result, store = run_pipeline(tmp_path, client)

    outcomes = [record.outcome for record in result.records]
    assert outcomes == ["failed", "fetched"]
    failed = result.records[0]
    assert failed.reason is not None and failed.reason.startswith("timeout:")
    assert result.failed and result.counts["failed"] == 1
    assert result.counts["fetched"] == 1
    written = store.list_keys("retail-raw", "raw_external/")
    assert len(written) == 2  # only the successful request's objects


def test_pipeline_requires_a_store_when_not_dry_run(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    with pytest.raises(UsageError, match="object store"):
        ExternalPipeline(None, settings, [StubClient([fx_request()])], today=DAY)


def test_failed_run_still_writes_a_manifest(tmp_path: Path) -> None:
    client = StubClient(
        [fx_request()],
        ApiRetryError("HTTP 500", attempts=4, status_code=500, reason="http_500"),
    )
    result, store = run_pipeline(tmp_path, client)

    assert result.manifest_path is not None and result.manifest_path.is_file()
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert payload["kind"] == "external_api_ingestion"
    assert payload["counts"]["failed"] == 1
    uploaded = json.loads(store.get_bytes("retail-raw", f"_manifests/{result.run_id}.json"))
    assert uploaded["run_id"] == result.run_id


def test_manifest_counts_and_run_id_prefix(tmp_path: Path) -> None:
    client = StubClient([fx_request(), fx_request("2026-10-05")])
    result, _ = run_pipeline(tmp_path, client)
    assert result.run_id.startswith("ext-")
    assert result.manifest["request_count"] == 2
    assert result.manifest["counts"] == {
        "fetched": 2,
        "already_fetched": 0,
        "failed": 0,
        "planned": 0,
    }
