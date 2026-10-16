"""Unit tests for the contract drift detector.

The happy-path test probes the real ``data_contracts/`` directory and asserts
the repository is drift-free; the negative-path tests fabricate tiny contract
directories (or mutate the real ``sales`` contract in a temp dir) to force each
of the five drift classes the detector must catch:

1. declared rule with no runtime implementation;
2. implemented rule with no declared counterpart (including referential);
3. source mapping missing / orphaned;
4. contract version mismatch (bad semver, unsupported major, duplicate id);
5. unsupported column (reserved provenance name, missing Bronze partition
   column).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

import mri.governance.drift as drift
from mri.governance.drift import DriftReport, run_drift_report

REPO_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = REPO_ROOT / "data_contracts"


def _write_contract(
    directory: Path,
    identifier: str,
    *,
    version: str = "1.0.0",
    columns: list[dict[str, Any]] | None = None,
    declared: list[str] | None = None,
    primary: list[str] | None = None,
    filename: str | None = None,
) -> Path:
    path = directory / (filename or f"{identifier}.yml")
    data: dict[str, Any] = {
        "id": identifier,
        "version": version,
        "owner": "test",
        "source": "test",
        "description": "fabricated for the drift tests",
        "format": {"types": ["csv"], "encoding": "utf-8", "delimiter": ","},
        "keys": {"primary": primary or ["widget_id"]},
        "columns": columns
        or [
            {
                "name": "widget_id",
                "type": "string",
                "required": True,
                "nullable": False,
            }
        ],
        "quality": {"declared": declared or ["widget_id is unique and not null"]},
        "sla": {"consumers": ["bronze"]},
    }
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def _sources(report: DriftReport) -> set[str]:
    return {issue.source for issue in report.issues}


def _kinds(report: DriftReport) -> set[str]:
    return {issue.kind for issue in report.issues}


def _real_sales(directory: Path) -> Path:
    """Copy the real sales contract into a temp dir, dropping one declared line."""
    raw = yaml.safe_load((CONTRACTS_DIR / "sales.yml").read_text(encoding="utf-8"))
    return _write(
        directory,
        raw,
        drop={"quantity >= 1"},
    )


def _write(directory: Path, data: dict[str, Any], *, drop: set[str]) -> Path:
    declared = [line for line in data["quality"]["declared"] if line not in drop]
    data["quality"]["declared"] = declared
    path = directory / f"{data['id']}.yml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def test_repo_contracts_are_drift_free() -> None:
    report = run_drift_report(CONTRACTS_DIR)
    assert report.sources_checked >= 14
    assert report.declared_rules >= 40
    assert report.implemented_rules >= 40
    assert report.issues == ()
    assert report.warnings == ()
    assert report.passed is True


def test_check_returns_true_for_the_repository() -> None:
    assert run_drift_report(CONTRACTS_DIR).passed is True
    assert drift.check(CONTRACTS_DIR) is True


def test_declared_rule_without_implementation_is_flagged(tmp_path: Path) -> None:
    _write_contract(
        tmp_path,
        "widgets",
        columns=[
            {"name": "widget_id", "type": "string", "required": True, "nullable": False},
            {"name": "amount", "type": "number", "required": True, "nullable": False},
        ],
        declared=["widget_id is unique and not null", "amount between 0 and 100"],
    )
    report = run_drift_report(tmp_path, source_tables={"widgets": "widgets"})
    assert _kinds(report) == {"declared_not_implemented"}
    issue = report.issues[0]
    assert issue.source == "widgets"
    assert "amount between 0 and 100" in issue.detail


def test_implemented_rule_without_declaration_is_flagged(tmp_path: Path) -> None:
    _real_sales(tmp_path)
    report = run_drift_report(tmp_path, source_tables={"sales": "sales"})
    assert _kinds(report) == {"implemented_not_declared"}
    assert "quantity >= 1" in report.issues[0].detail


def test_implemented_reference_without_declaration_is_flagged(tmp_path: Path) -> None:
    raw = yaml.safe_load((CONTRACTS_DIR / "sales.yml").read_text(encoding="utf-8"))
    _write(tmp_path, raw, drop={"store_id exists in stores"})
    report = run_drift_report(tmp_path, source_tables={"sales": "sales"})
    assert _kinds(report) == {"implemented_not_declared"}
    assert "store_id exists in stores" in report.issues[0].detail


def test_source_mapping_missing_is_flagged(tmp_path: Path) -> None:
    _write_contract(tmp_path, "widgets")
    report = run_drift_report(tmp_path)
    assert "widgets" in _sources(report)
    assert "source_mapping_missing" in _kinds(report)


def test_source_mapping_orphan_is_flagged(tmp_path: Path) -> None:
    _real_sales(tmp_path)
    report = run_drift_report(tmp_path, source_tables={"sales": "sales", "phantom": "phantom"})
    assert "source_mapping_orphan" in _kinds(report)
    assert "phantom" in _sources(report)


def test_unsupported_major_version_is_flagged(tmp_path: Path) -> None:
    _write_contract(tmp_path, "widgets", version="2.0.0")
    report = run_drift_report(tmp_path, source_tables={"widgets": "widgets"})
    assert "version_mismatch" in _kinds(report)


def test_invalid_semver_is_flagged(tmp_path: Path) -> None:
    _write_contract(tmp_path, "widgets", version="1.0")
    report = run_drift_report(tmp_path, source_tables={"widgets": "widgets"})
    assert "version_mismatch" in _kinds(report)


def test_duplicate_contract_id_is_flagged(tmp_path: Path) -> None:
    _write_contract(tmp_path, "widgets", version="1.0.0", filename="widgets.yml")
    _write_contract(tmp_path, "widgets", version="1.1.0", filename="widgets2.yml")
    report = run_drift_report(tmp_path, source_tables={"widgets": "widgets"})
    messages = {issue.detail for issue in report.issues}
    assert "version_mismatch" in _kinds(report)
    assert any("two files" in message for message in messages)


def test_reserved_provenance_column_is_flagged(tmp_path: Path) -> None:
    _write_contract(
        tmp_path,
        "widgets",
        columns=[
            {"name": "widget_id", "type": "string", "required": True, "nullable": False},
            {"name": "batch_id", "type": "string", "required": True, "nullable": False},
        ],
    )
    report = run_drift_report(tmp_path, source_tables={"widgets": "widgets"})
    assert "unsupported_column" in _kinds(report)
    assert "batch_id" in report.issues[0].detail


def test_missing_bronze_partition_column_is_flagged(tmp_path: Path) -> None:
    _write_contract(tmp_path, "widgets")
    report = run_drift_report(
        tmp_path,
        source_tables={"widgets": "widgets"},
        partition_columns={"widgets": "mystery"},
    )
    assert "unsupported_column" in _kinds(report)
    assert "mystery" in report.issues[0].detail


def test_declared_not_null_on_nullable_key_is_flagged(tmp_path: Path) -> None:
    _write_contract(
        tmp_path,
        "widgets",
        columns=[{"name": "widget_id", "type": "string", "required": True, "nullable": True}],
        declared=["widget_id is unique and not null"],
    )
    report = run_drift_report(tmp_path, source_tables={"widgets": "widgets"})
    assert _kinds(report) == {"declared_not_implemented"}
    assert "widget_id" in report.issues[0].detail


def test_bronze_only_documented_exception_warns_but_does_not_fail(
    tmp_path: Path, monkeypatch: Any
) -> None:
    monkeypatch.setattr(drift, "BRONZE_ONLY_SOURCES", frozenset({"widgets"}))
    _write_contract(tmp_path, "widgets")
    report = run_drift_report(tmp_path, source_tables={})
    assert report.passed is True
    assert report.issues == ()
    assert any(entry.kind == "source_mapping_missing" for entry in report.warnings)


def test_declared_lines_of_every_repo_contract_are_classifiable() -> None:
    from mri.ingestion.contracts import load_all_contracts

    for contract in load_all_contracts().values():
        _rules, unparsed = drift.classifiable_declared(contract)
        assert unparsed == [], f"{contract.id}: {unparsed}"
