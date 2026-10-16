"""CLI tests for the ``external`` subcommand (offline).

Dry-run paths run for real without network or MinIO; the write paths use
stubbed clients and an in-memory object store injected via monkeypatch.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from mri.ingestion import cli as cli_module
from mri.ingestion.clients.base import FetchOptions, FetchOutcome, FetchRequest
from mri.ingestion.clients.fx_client import SOURCE as FX_SOURCE
from mri.ingestion.clients.product_catalog_client import SOURCE as PRODUCTS_SOURCE
from mri.ingestion.clients.weather_client import SOURCE_FORECAST, SOURCE_HISTORY
from mri.ingestion.config import Settings
from mri.ingestion.exceptions import ApiRetryError
from mri.ingestion.external import request_hash
from mri.ingestion.storage import InMemoryObjectStore

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2


class CliStubClient:
    sources: tuple[str, ...] = (FX_SOURCE,)

    def __init__(self, *, fail: bool = False) -> None:
        self._fail = fail
        self.fetch_calls = 0

    def build_requests(self, options: FetchOptions) -> list[FetchRequest]:
        return [
            FetchRequest(
                source=FX_SOURCE,
                data_type="fx_rates",
                url="https://fx.test/v2/rates",
                params={"base": "EUR", "quotes": "MAD,USD,GBP"},
            )
        ]

    def fetch(self, request: FetchRequest) -> FetchOutcome:
        self.fetch_calls += 1
        if self._fail:
            raise ApiRetryError("HTTP 500", attempts=4, status_code=500, reason="http_500")
        return FetchOutcome(
            request=request,
            payload=[],
            raw=b'[{"date":"2026-10-07","base":"EUR","quote":"MAD","rate":11.2}]',
            status_code=200,
            attempts=1,
            elapsed_ms=4,
        )


def run_cli(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    code = cli_module.main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


# --------------------------------------------------------------------------
# dry-run
# --------------------------------------------------------------------------


def test_external_dry_run_fx_json_plan(capsys: pytest.CaptureFixture[str]) -> None:
    code, out, _err = run_cli(
        ["--json", "external", "--source", "fx", "--date", "2026-10-06", "--dry-run"],
        capsys,
    )
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["kind"] == "external_api_ingestion"
    assert payload["dry_run"] is True
    assert payload["counts"]["planned"] == 1
    record = payload["records"][0]
    assert record["source"] == FX_SOURCE
    assert record["params"]["date"] == "2026-10-06"
    assert record["object_key"].startswith("raw_external/source=frankfurter_fx/")


def test_external_dry_run_weather_selects_both_datasets(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, out, _err = run_cli(
        [
            "--json",
            "external",
            "--source",
            "weather",
            "--location",
            "Casablanca",
            "--date",
            "2024-06-01",
            "--dry-run",
        ],
        capsys,
    )
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["counts"]["planned"] == 2
    assert {record["source"] for record in payload["records"]} == {
        SOURCE_HISTORY,
        SOURCE_FORECAST,
    }
    hashes = {record["request_hash"] for record in payload["records"]}
    assert len(hashes) == 2


def test_external_dry_run_products_from_barcode_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    barcodes = tmp_path / "barcodes.txt"
    barcodes.write_text("# enrich these\n3017620422003\n5000112637922\n", encoding="utf-8")
    code, out, _err = run_cli(
        [
            "--json",
            "external",
            "--source",
            "products",
            "--barcode-file",
            str(barcodes),
            "--dry-run",
        ],
        capsys,
    )
    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["counts"]["planned"] == 2
    assert {record["source"] for record in payload["records"]} == {PRODUCTS_SOURCE}


# --------------------------------------------------------------------------
# usage errors (exit code 2)
# --------------------------------------------------------------------------


def test_products_without_barcodes_is_a_usage_error(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, _out, err = run_cli(["external", "--source", "products", "--dry-run"], capsys)
    assert code == EXIT_USAGE
    assert "barcode" in err


def test_unknown_source_is_a_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    code, _out, err = run_cli(["external", "--source", "nope", "--dry-run"], capsys)
    assert code == EXIT_USAGE
    assert "unknown --source" in err


def test_unknown_location_is_a_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    code, _out, err = run_cli(
        ["external", "--source", "weather", "--location", "Nowhere", "--dry-run"], capsys
    )
    assert code == EXIT_USAGE
    assert "unknown --location" in err


def test_bad_date_is_a_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    code, _out, err = run_cli(
        ["external", "--source", "fx", "--date", "07/10/2026", "--dry-run"], capsys
    )
    assert code == EXIT_USAGE
    assert "--date must be YYYY-MM-DD" in err


def test_non_positive_limit_is_a_usage_error(capsys: pytest.CaptureFixture[str]) -> None:
    code, _out, err = run_cli(["external", "--source", "fx", "--limit", "0", "--dry-run"], capsys)
    assert code == EXIT_USAGE
    assert "--limit" in err


# --------------------------------------------------------------------------
# write paths (stubbed clients + in-memory store)
# --------------------------------------------------------------------------


def _isolate_manifests(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Keep run manifests inside pytest's tmp dir instead of reports/."""
    manifest_dir = tmp_path / "manifests"
    base = Settings.from_env()
    monkeypatch.setattr(
        cli_module,
        "_settings",
        lambda args: replace(base, manifest_dir=manifest_dir),
    )
    return manifest_dir


def test_external_run_success_exits_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    manifest_dir = _isolate_manifests(monkeypatch, tmp_path)
    client = CliStubClient()
    store = InMemoryObjectStore()
    monkeypatch.setattr(cli_module, "_build_clients", lambda settings, options: [client])
    monkeypatch.setattr(cli_module, "_make_store", lambda settings: store)

    code, out, _err = run_cli(["external", "--source", "fx"], capsys)

    assert code == EXIT_OK
    assert "fetched=1" in out
    assert client.fetch_calls == 1
    assert len(store.list_keys("retail-raw", "raw_external/")) == 2
    assert store.list_keys("retail-raw", "_manifests/")
    assert [path.name for path in manifest_dir.glob("ext-*.json")]


def test_external_run_reports_failure_with_exit_one(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _isolate_manifests(monkeypatch, tmp_path)
    client = CliStubClient(fail=True)
    monkeypatch.setattr(cli_module, "_build_clients", lambda settings, options: [client])
    monkeypatch.setattr(cli_module, "_make_store", lambda settings: InMemoryObjectStore())

    code, out, _err = run_cli(["external", "--source", "fx"], capsys)

    assert code == EXIT_FAILURE
    assert "failed=1" in out
    assert "http_500" in out


def test_dry_run_never_builds_a_store(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def explode(settings: Any) -> Any:
        raise AssertionError("dry-run must not create an object store")

    monkeypatch.setattr(cli_module, "_make_store", explode)
    code, _out, _err = run_cli(["external", "--source", "fx", "--dry-run"], capsys)
    assert code == EXIT_OK


# --------------------------------------------------------------------------
# shared request/hash consistency
# --------------------------------------------------------------------------


def test_planned_keys_match_pipeline_hashing(capsys: pytest.CaptureFixture[str]) -> None:
    code, out, _err = run_cli(
        ["--json", "external", "--source", "fx", "--date", "2026-10-06", "--dry-run"],
        capsys,
    )
    assert code == EXIT_OK
    record = json.loads(out)["records"][0]
    expected = request_hash(
        FetchRequest(
            source=FX_SOURCE,
            data_type="fx_rates",
            url=record["endpoint"],
            params=record["params"],
        )
    )
    assert record["request_hash"] == expected
