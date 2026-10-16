"""Phase 16: unit tests for the ``apps.observability`` CLI and the ingestion hook.

The ``record`` / ``metrics`` / ``trace`` commands are fully offline; ``sync``
needs PostgreSQL and fails loudly without it (mirroring live-run gating).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from mri.observability.cli import EXIT_FAILURE, EXIT_OK, EXIT_USAGE, main
from mri.observability.store import EventLog

SALES_CSV = (
    Path(__file__).resolve().parents[2] / "data" / "generated" / "tiny" / "sales" / "sales.csv"
)


def _record(log: Path, dag_run_id: str, pipeline: str = "airflow") -> int:
    return main(
        [
            "--events-dir",
            str(log),
            "record",
            "--dag-run-id",
            dag_run_id,
            "--pipeline",
            pipeline,
            "--stage",
            "dag_run",
            "--status",
            "success",
            "--message",
            "DAG run completed",
        ]
    )


def test_record_writes_one_event_to_the_log(tmp_path: Path) -> None:
    log = tmp_path / "obs"
    assert _record(log, "dag__1") == EXIT_OK
    events = EventLog.resolve(log).read_events()
    assert len(events) == 1
    event = events[0]
    assert event.dag_run_id == "dag__1"
    assert event.pipeline == "airflow"
    assert event.stage == "dag_run"
    assert event.status == "success"
    assert event.run_id.startswith("airflow-")


def test_metrics_cli_is_offline_and_matches_the_statistics(tmp_path: Path, capsys) -> None:
    _record(tmp_path / "obs", "dag__1")
    capsys.readouterr()  # flush the record command's stdout banner
    assert main(["--json", "--events-dir", str(tmp_path / "obs"), "metrics"]) == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["overall"]["runs"] == 1
    assert payload["overall"]["succeeded"] == 1
    assert payload["stages"][0]["pipeline"] == "airflow"


def test_trace_by_dag_run_id_returns_the_ordered_events(tmp_path: Path, capsys) -> None:
    log = tmp_path / "obs"
    _record(log, "dag__1", pipeline="ingestion")
    _record(log, "dag__1", pipeline="silver")
    _record(log, "dag__2", pipeline="warehouse")
    assert main(["--events-dir", str(log), "trace", "--dag-run-id", "dag__1"]) == EXIT_OK
    output = capsys.readouterr().out
    assert "ingestion/dag_run" in output
    assert "silver/dag_run" in output


def test_trace_by_run_id_and_batch_id(tmp_path: Path, capsys) -> None:
    log = tmp_path / "obs"
    main(
        [
            "--events-dir",
            str(log),
            "record",
            "--pipeline",
            "ingestion",
            "--stage",
            "ingest",
            "--run-id",
            "run-123",
            "--batch-id",
            "sales",
            "--message",
            "batch ok",
        ]
    )
    main(
        [
            "--events-dir",
            str(log),
            "record",
            "--pipeline",
            "bronze",
            "--stage",
            "bronze",
            "--run-id",
            "run-456",
            "--batch-id",
            "products",
            "--message",
            "batch ok",
        ]
    )
    assert main(["--events-dir", str(log), "trace", "--run-id", "run-123"]) == EXIT_OK
    assert "run-123" in capsys.readouterr().out
    assert main(["--events-dir", str(log), "trace", "--batch-id", "products"]) == EXIT_OK
    assert "products" in capsys.readouterr().out


def test_trace_requires_a_selector(tmp_path: Path) -> None:
    assert main(["--events-dir", str(tmp_path), "trace"]) == EXIT_USAGE


def test_trace_reports_no_matches_with_failure_code(tmp_path: Path, capsys) -> None:
    assert main(["--events-dir", str(tmp_path), "trace", "--run-id", "nope"]) == EXIT_FAILURE
    assert "no events matched" in capsys.readouterr().err


def test_sync_fails_loudly_without_postgres(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("POSTGRES_PASSWORD", "")
    log = tmp_path / "obs"
    _record(log, "dag__1")
    assert main(["--events-dir", str(log), "sync"]) == EXIT_FAILURE


@pytest.mark.skipif(not SALES_CSV.is_file(), reason="tiny sales generator output missing")
def test_validate_path_emits_a_correlated_run(tmp_path: Path) -> None:
    """Real code path: the ingestion CLI validates one file and writes events."""
    from mri.ingestion import cli as ingestion_cli

    log = tmp_path / "obs"
    code = ingestion_cli.main(
        [
            "--observability-dir",
            str(log),
            "--dag-run-id",
            "dag__validate_1",
            "validate",
            "--path",
            str(SALES_CSV),
        ]
    )
    assert code == 0
    events = EventLog.resolve(log).read_events()
    assert len(events) == 3  # started + per-file + finished
    assert all(event.dag_run_id == "dag__validate_1" for event in events)
    assert events[0].status == "started"
    assert events[1].status == "success"
    assert events[1].source == "sales"
    assert events[1].batch_id == "sales"
    assert events[-1].status == "success"
    assert events[-1].duration_ms is not None
    assert all(event.run_id == events[0].run_id for event in events)
