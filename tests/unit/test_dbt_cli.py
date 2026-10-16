"""Unit tests for the repository dbt wrapper (``python -m apps.dbt``).

These tests need neither dbt nor a database: they pin the wrapper contract —
``DBT_TARGET_*`` injection without overriding exported values, automatic
``--project-dir``/``--profiles-dir`` flags (unless the caller supplied them),
executable resolution next to the current interpreter, the loud failure when
no password is configured, and transparent forwarding of dbt's exit code.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

import apps.dbt
from apps.dbt import (
    MISSING_PASSWORD_MESSAGE,
    _emit_model_events,
    build_command,
    build_env,
    dbt_executable,
    extract_observability_args,
    main,
)
from mri.ingestion.config import Settings
from mri.observability.events import PipelineEvent


def _settings(password: str = "secret", **overrides: str) -> Settings:
    base = {
        "POSTGRES_HOST": "db.example",
        "POSTGRES_PORT": "6543",
        "POSTGRES_DB": "retail_test",
        "POSTGRES_USER": "loader",
        "POSTGRES_PASSWORD": password,
        "POSTGRES_SSLMODE": "require",
    }
    base.update(overrides)
    return Settings.from_env(base)


def test_build_env_injects_connection_defaults_from_settings() -> None:
    env = build_env(_settings(), env={})
    assert env["DBT_TARGET_HOST"] == "db.example"
    assert env["DBT_TARGET_PORT"] == "6543"
    assert env["DBT_TARGET_DB"] == "retail_test"
    assert env["DBT_TARGET_USER"] == "loader"
    assert env["DBT_TARGET_PASSWORD"] == "secret"
    assert env["DBT_TARGET_SSLMODE"] == "require"


def test_build_env_never_overrides_explicitly_exported_values() -> None:
    exported = {"DBT_TARGET_HOST": "exported.host", "DBT_TARGET_PASSWORD": "exported-secret"}
    env = build_env(_settings(), env=exported)
    assert env["DBT_TARGET_HOST"] == "exported.host"
    assert env["DBT_TARGET_PASSWORD"] == "exported-secret"
    assert env["DBT_TARGET_DB"] == "retail_test"
    assert env["DBT_TARGET_PORT"] == "6543"


def test_build_env_starts_from_the_supplied_environment() -> None:
    env = build_env(_settings(), env={"PATH": "/usr/bin", "DBT_THREADS": "8"})
    assert env["PATH"] == "/usr/bin"
    assert env["DBT_THREADS"] == "8"
    assert env["DBT_TARGET_USER"] == "loader"


def test_build_command_appends_project_and_profiles_flags(tmp_path: Path) -> None:
    command = build_command(["build"], repo_root=tmp_path, executable="dbt")
    assert command == [
        "dbt",
        "build",
        "--project-dir",
        str(tmp_path / "dbt"),
        "--profiles-dir",
        str(tmp_path / "dbt"),
    ]


def test_build_command_keeps_user_supplied_project_flags(tmp_path: Path) -> None:
    command = build_command(
        ["build", "--project-dir", "/custom", "--profiles-dir", "/custom"],
        repo_root=tmp_path,
        executable="dbt",
    )
    assert command == ["dbt", "build", "--project-dir", "/custom", "--profiles-dir", "/custom"]

    partial = build_command(
        ["--vars", "{schema_suffix: _it1}", "build", "--project-dir", "/custom"],
        repo_root=tmp_path,
        executable="dbt",
    )
    assert partial.count("--project-dir") == 1
    assert "--profiles-dir" in partial


def test_dbt_executable_prefers_the_interpreter_sibling(tmp_path: Path, monkeypatch) -> None:
    executable = tmp_path / ("dbt.exe" if sys.platform == "win32" else "dbt")
    executable.write_text("", encoding="utf-8")
    monkeypatch.setattr(sys, "executable", str(tmp_path / "python.exe"))
    assert dbt_executable() == str(executable)


def test_dbt_executable_falls_back_to_path_lookup(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(sys, "executable", str(tmp_path / "python.exe"))
    monkeypatch.setattr(apps.dbt.shutil, "which", lambda _name: "/opt/venv/bin/dbt")
    assert dbt_executable() == "/opt/venv/bin/dbt"


def test_dbt_executable_fails_loudly_when_dbt_is_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(sys, "executable", str(tmp_path / "python.exe"))
    monkeypatch.setattr(apps.dbt.shutil, "which", lambda _name: None)
    with pytest.raises(FileNotFoundError, match="dbt executable not found"):
        dbt_executable()


def test_main_returns_one_when_no_password_is_configured(monkeypatch, capsys) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "")
    assert main(["build"]) == 1
    captured = capsys.readouterr()
    assert MISSING_PASSWORD_MESSAGE in captured.err


def test_main_forwards_dbt_exit_code_and_connection_env(monkeypatch) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.delenv("DBT_TARGET_PASSWORD", raising=False)
    captured: dict[str, object] = {}

    def _fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return SimpleNamespace(returncode=7)

    monkeypatch.setattr(apps.dbt, "build_command", lambda argv: ["dbt", *argv])
    monkeypatch.setattr(apps.dbt.subprocess, "run", _fake_run)
    assert main(["build"]) == 7
    assert captured["command"] == ["dbt", "build"]
    env = captured["env"]
    assert isinstance(env, dict)
    assert env["DBT_TARGET_PASSWORD"] == "secret"
    assert captured["check"] is False


def test_extract_observability_args_strips_only_the_two_flags() -> None:
    forwarded, dag_run_id, directory = extract_observability_args(
        [
            "build",
            "--dag-run-id",
            "dag__1",
            "--vars",
            "{schema_suffix: _it1}",
            "--observability-dir",
            "reports/obs",
        ]
    )
    assert forwarded == ["build", "--vars", "{schema_suffix: _it1}"]
    assert dag_run_id == "dag__1"
    assert directory is not None
    assert Path(directory) == Path("reports/obs")


def test_extract_observability_args_returns_empties_when_absent() -> None:
    forwarded, dag_run_id, directory = extract_observability_args(["build", "--threads", "4"])
    assert forwarded == ["build", "--threads", "4"]
    assert dag_run_id == ""
    assert directory is None


def test_extract_observability_args_rejects_missing_value() -> None:
    with pytest.raises(ValueError, match="--dag-run-id requires a value"):
        extract_observability_args(["build", "--dag-run-id"])
    with pytest.raises(ValueError, match="--observability-dir requires a value"):
        extract_observability_args(["--observability-dir"])


def test_emit_model_events_only_sets_error_code_on_errors(monkeypatch) -> None:
    """Pass/skip are normal; only error/fail models carry DBT_MODEL_FAILED."""
    from mri.observability import runtime

    runtime.reset_context()
    captured: list[PipelineEvent] = []

    def _capture(**kwargs):
        captured.append(PipelineEvent(timestamp="2026-10-09T13:00:00Z", **kwargs))

    monkeypatch.setattr(runtime, "emit", _capture)
    payload = {
        "results": [
            {"unique_id": "model.mri.stg_sales", "status": "success", "execution_time": 0.4},
            {"unique_id": "test.mri.unique_stg_sales_sale_id", "status": "pass"},
            {"unique_id": "model.mri.fact_ventes", "status": "error", "message": "boom"},
            {"unique_id": "test.mri.not_null_fact_ventes_sale_id", "status": "skipped"},
        ]
    }
    _emit_model_events("dbt-r1", payload)
    assert [e.error_code for e in captured] == ["", "", "DBT_MODEL_FAILED", ""]
    assert [e.status for e in captured] == ["success", "pass", "error", "skipped"]


def test_main_strips_correlation_flags_before_dbt(monkeypatch, tmp_path: Path) -> None:
    """Airflow passes explicit flags; dbt itself must never receive them."""
    from mri.observability import runtime

    runtime.reset_context()
    monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
    monkeypatch.delenv("DBT_TARGET_PASSWORD", raising=False)
    log = tmp_path / "obs"
    captured: dict[str, object] = {}

    def _fake_run(command, **kwargs):
        captured["command"] = command
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(apps.dbt, "build_command", lambda argv: ["dbt", *argv])
    monkeypatch.setattr(apps.dbt.subprocess, "run", _fake_run)
    assert (
        main(
            [
                "build",
                "--dag-run-id",
                "dag__airflow",
                "--observability-dir",
                str(log),
            ]
        )
        == 0
    )
    assert captured["command"] == ["dbt", "build"]
    events = list((log / "events.jsonl").parent.glob("events.jsonl"))
    assert events, "correlated dbt runs must emit events"
    from mri.observability.store import EventLog

    stored = EventLog.resolve(log).read_events()
    assert any(event.status == "started" for event in stored)
    assert any(event.status == "success" for event in stored)
    assert all(event.dag_run_id == "dag__airflow" for event in stored)
