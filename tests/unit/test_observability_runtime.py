"""Phase 16: unit tests for the correlation context and emission routing."""

from __future__ import annotations

from pathlib import Path

import pytest

from mri.observability import runtime
from mri.observability.store import EventLog


@pytest.fixture(autouse=True)
def _clean_context() -> None:
    runtime.reset_context()


def test_set_and_get_context() -> None:
    runtime.set_context(dag_run_id="dag__1", events_dir="logs")
    context = runtime.get_context()
    assert context.dag_run_id == "dag__1"
    assert context.events_dir == Path("logs")


def test_set_context_keeps_unspecified_values() -> None:
    runtime.set_context(dag_run_id="dag__1", events_dir="logs")
    runtime.set_context(dag_run_id="dag__2")
    assert runtime.get_context().dag_run_id == "dag__2"
    assert runtime.get_context().events_dir == Path("logs")


def test_reset_context_clears_everything() -> None:
    runtime.set_context(dag_run_id="dag__1", events_dir="logs")
    runtime.reset_context()
    assert runtime.get_context() == runtime.RuntimeContext()


def test_emit_appends_to_the_configured_directory(tmp_path: Path) -> None:
    runtime.set_context(dag_run_id="dag__1", events_dir=str(tmp_path))
    event = runtime.emit(
        pipeline="ingestion", stage="ingest", run_id="r1", status="success", batch_id="sales"
    )
    assert event.dag_run_id == "dag__1"
    stored = EventLog.resolve(tmp_path).read_events()
    assert [stored_event.run_id for stored_event in stored] == ["r1"]
    assert stored[0].dag_run_id == "dag__1"


def test_emit_context_missing_dag_run_id_is_empty(tmp_path: Path) -> None:
    runtime.set_context(events_dir=str(tmp_path))
    event = runtime.emit(pipeline="bronze", stage="bronze", run_id="r1", status="started")
    assert event.dag_run_id == ""


def test_emit_redacts_secret_payload(tmp_path: Path) -> None:
    runtime.set_context(events_dir=str(tmp_path))
    runtime.emit(
        pipeline="ingestion",
        stage="external",
        run_id="r1",
        status="success",
        payload={"access_key": "AKIA", "host": "fx.example"},
    )
    stored = EventLog.resolve(tmp_path).read_events()[0]
    assert stored.payload["access_key"] == "***"
    assert stored.payload["host"] == "fx.example"


def test_trace_across_one_dag_run_threads_run_and_batch(monkeypatch, tmp_path: Path) -> None:
    """The core Phase 16 guarantee: one DAG run, three pipelines, one trace."""
    runtime.set_context(dag_run_id="dag__nightly", events_dir=str(tmp_path))
    runtime.emit(
        pipeline="ingestion",
        stage="ingest",
        run_id="run-ingest",
        status="success",
        source="sales",
        batch_id="sales",
        records_read=100,
    )
    runtime.emit(
        pipeline="silver",
        stage="silver",
        run_id="run-load",
        status="success",
        source="sales",
        batch_id="sales",
        records_read=100,
        records_written=99,
    )
    runtime.emit(
        pipeline="warehouse",
        stage="warehouse",
        run_id="run-build",
        status="failed",
        source="sales",
        batch_id="fact_sales",
        error_code="WAREHOUSE_UNBALANCED",
    )
    events = EventLog.resolve(tmp_path).read_events()
    assert {event.pipeline for event in events} == {"ingestion", "silver", "warehouse"}
    assert all(event.dag_run_id == "dag__nightly" for event in events)
    sheet_sales = [event for event in events if event.batch_id == "sales"]
    assert {event.pipeline for event in sheet_sales} == {"ingestion", "silver"}
    assert any(event.error_code == "WAREHOUSE_UNBALANCED" for event in events)


def test_elapsed_ms_returns_whole_milliseconds() -> None:
    from time import perf_counter

    begin = perf_counter()
    assert runtime.elapsed_ms(begin) >= 0


def test_emit_returns_the_serialised_event(tmp_path: Path) -> None:
    runtime.set_context(events_dir=str(tmp_path))
    event = runtime.emit(pipeline="dbt", stage="dbt", run_id="dbt-1", status="started")
    assert isinstance(event, runtime.PipelineEvent)
    assert event.run_id == "dbt-1"
