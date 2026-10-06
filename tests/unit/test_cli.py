"""CLI end-to-end tests (python -m apps.data_generator contract)."""

from __future__ import annotations

import json

import pytest

from mri.synthetic.cli import main


def test_cli_generates_dataset(tmp_path):
    code = main(["--scale", "tiny", "--seed", "42", "--output", str(tmp_path), "--quiet"])
    assert code == 0
    manifest = json.loads((tmp_path / "tiny" / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["row_counts"]["sales"] == 2_000
    assert (tmp_path / "tiny" / "sales" / "sales.csv").exists()


def test_cli_is_deterministic(tmp_path):
    main(["--scale", "tiny", "--seed", "7", "--output", str(tmp_path / "a"), "--quiet"])
    main(["--scale", "tiny", "--seed", "7", "--output", str(tmp_path / "b"), "--quiet"])
    first = json.loads((tmp_path / "a" / "tiny" / "manifest.json").read_text(encoding="utf-8"))
    second = json.loads((tmp_path / "b" / "tiny" / "manifest.json").read_text(encoding="utf-8"))
    assert first["files"] == second["files"]


def test_cli_quiet_produces_no_stdout(tmp_path, capsys):
    main(["--scale", "tiny", "--seed", "42", "--output", str(tmp_path), "--quiet"])
    assert capsys.readouterr().out == ""


def test_cli_report_lists_counts(tmp_path, capsys):
    main(["--scale", "tiny", "--seed", "42", "--output", str(tmp_path)])
    out = capsys.readouterr().out
    assert "synthetic data generator" in out
    assert "sales" in out
    assert "Controlled synthetic data" in out


def test_cli_dq_fixture(tmp_path):
    code = main(
        ["--dq-fixture", "null_required", "--dq-output", str(tmp_path), "--seed", "42", "--quiet"]
    )
    assert code == 0
    assert (tmp_path / "null_required" / "sales" / "sales.csv").exists()


def test_cli_rejects_unknown_scale(tmp_path):
    with pytest.raises(SystemExit) as excinfo:
        main(["--scale", "gigantic", "--output", str(tmp_path)])
    assert excinfo.value.code == 2
