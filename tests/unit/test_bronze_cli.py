"""Unit tests for the ``bronze`` CLI subcommand (usage errors and summaries).

An :class:`mri.ingestion.storage.InMemoryObjectStore` is injected via
monkeypatch so no network is touched; behavior matches the ingestion and
external CLI tests (exit codes 0/1/2, ``--json`` manifests).
"""

from __future__ import annotations

import argparse
import json
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
from bronze_fixtures import CONTRACTS, make_settings, region_row, seed_raw

from mri.ingestion import cli as cli_module
from mri.ingestion.storage import InMemoryObjectStore

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2


def run_bronze(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    code = cli_module.main(argv)
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _patch_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> tuple[Path, Path]:
    manifests_ingest = tmp_path / "manifests_ingest"
    manifests_bronze = tmp_path / "manifests_bronze"
    manifests_ingest.mkdir(parents=True, exist_ok=True)
    manifests_bronze.mkdir(parents=True, exist_ok=True)

    def _cli_settings(_args: cli_module.argparse.Namespace) -> cli_module.Settings:
        return replace(
            make_settings(tmp_path),
            contracts_dir=cli_module.Settings.from_env().contracts_dir,
            manifest_dir=manifests_ingest,
            bronze_manifest_dir=manifests_bronze,
        )

    monkeypatch.setattr(cli_module, "_settings", _cli_settings)
    return manifests_ingest, manifests_bronze


def test_bronze_help_documents_storage_paths_and_exit_codes() -> None:
    parser = cli_module.build_parser()
    subparsers = next(a for a in parser._actions if a.dest == "command")
    choices = cast(dict[str, argparse.ArgumentParser], subparsers.choices or {})
    epilog = choices["bronze"].epilog or ""
    assert "bronze/<source>" in epilog
    assert "_quality/" in epilog
    assert "_promoted/" in epilog
    assert "exit codes: 0 ok, 1 at least one batch failed, 2 usage error" in epilog


def test_bronze_all_combined_with_other_source_is_usage_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)
    monkeypatch.setattr(cli_module, "_make_store", lambda settings: InMemoryObjectStore())

    code, _out, err = run_bronze(["bronze", "--source", "all", "--source", "regions"], capsys)

    assert code == EXIT_USAGE
    assert "error:" in err


def test_bronze_unknown_source_is_usage_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)
    monkeypatch.setattr(cli_module, "_make_store", lambda settings: InMemoryObjectStore())

    code, _out, err = run_bronze(["bronze", "--source", "nope"], capsys)

    assert code == EXIT_USAGE
    assert "unknown source" in err


def test_bronze_dry_run_success_exits_zero_with_json_summary(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)
    store = InMemoryObjectStore()
    seed_raw(store, make_settings(tmp_path), "regions", [region_row()])
    monkeypatch.setattr(cli_module, "_make_store", lambda s: store)

    code, out, _err = run_bronze(["--json", "bronze", "--source", "regions", "--dry-run"], capsys)

    assert code == EXIT_OK
    payload = json.loads(out)
    assert payload["dry_run"] is True
    assert payload["counts"]["planned"] == 1
    assert payload["records"][0]["outcome"] == "planned"
    assert payload["records"][0]["source"] == "regions"


def test_bronze_run_success_exits_zero_with_human_summary(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)
    store = InMemoryObjectStore()
    seed_raw(store, make_settings(tmp_path), "regions", [region_row()])
    monkeypatch.setattr(cli_module, "_make_store", lambda s: store)

    code, out, _err = run_bronze(["bronze", "--source", "regions"], capsys)

    assert code == EXIT_OK
    assert "bronze run" in out
    assert "bronzed=" in out
    assert "[bronzed]" in out
    assert "regions/regions__" in out


def test_bronze_run_failure_exits_one(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    _patch_settings(monkeypatch, tmp_path)
    store = InMemoryObjectStore()
    settings = make_settings(tmp_path)
    contract = CONTRACTS["regions"]
    header = [c.name for c in contract.columns if c.name != "demand_weight"]
    payload = (",".join(header) + "\n" + ",".join("x" for _ in header) + "\n").encode("utf-8")
    store.put_bytes(settings.bucket_raw, "raw/regions/regions__bad.csv", payload)
    monkeypatch.setattr(cli_module, "_make_store", lambda s: store)

    code, out, _err = run_bronze(["bronze", "--batch", "raw/regions/regions__bad.csv"], capsys)

    assert code == EXIT_FAILURE
    assert "bronze run" in out
    assert "failed=" in out
    assert "[failed]" in out


def test_bronze_dry_run_writes_no_manifests_or_bronze_objects(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], tmp_path: Path
) -> None:
    manifests_ingest, manifests_bronze = _patch_settings(monkeypatch, tmp_path)
    store = InMemoryObjectStore()
    settings = make_settings(tmp_path)
    seed_raw(store, settings, "regions", [region_row()])
    monkeypatch.setattr(cli_module, "_make_store", lambda s: store)

    code, _out, _err = run_bronze(["bronze", "--source", "regions", "--dry-run"], capsys)

    assert code == EXIT_OK
    assert list(manifests_ingest.iterdir()) == []
    assert list(manifests_bronze.iterdir()) == []
    assert not store.bucket_exists(settings.bucket_bronze)
