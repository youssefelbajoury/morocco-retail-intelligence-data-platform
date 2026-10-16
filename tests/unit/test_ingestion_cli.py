"""CLI tests for the ingestion entry point."""

from __future__ import annotations

import json
from pathlib import Path

from mri.ingestion.cli import main

VALID = "region_id,region_name,demand_weight\nR01,Alpha,0.5\n"


def _write(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_cli_contracts(capsys) -> None:
    assert main(["contracts"]) == 0
    out = capsys.readouterr().out
    assert "14 contracts" in out
    assert "sales" in out


def test_cli_contracts_json(capsys) -> None:
    assert main(["--json", "contracts"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert {item["id"] for item in payload} >= {"sales", "regions"}


def test_cli_validate_ok(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "regions.csv", VALID)
    assert main(["validate", "--source", "regions", "--path", str(path)]) == 0
    assert "[OK]" in capsys.readouterr().out


def test_cli_validate_invalid(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "regions.csv", "region_id,region_name\nR01,Alpha\n")
    assert main(["validate", "--source", "regions", "--path", str(path)]) == 1
    assert "INVALID" in capsys.readouterr().out


def test_cli_ingest_dry_run(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "regions.csv", VALID)
    code = main(["ingest", "--source", "regions", "--path", str(path), "--dry-run"])
    assert code == 0
    assert "dry_run=True" in capsys.readouterr().out


def test_cli_ingest_dry_run_json(tmp_path: Path, capsys) -> None:
    path = _write(tmp_path / "regions.csv", VALID)
    assert main(["--json", "ingest", "--source", "regions", "--path", str(path), "--dry-run"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["counts"]["ingested"] == 1


def test_cli_manifest_list(capsys) -> None:
    assert main(["manifest", "--list"]) == 0
    assert "manifest" in capsys.readouterr().out
