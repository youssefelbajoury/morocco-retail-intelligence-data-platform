"""Phase 16: unit tests for offline pipeline metrics derived from one log."""

from __future__ import annotations

from datetime import UTC, datetime

from mri.observability.events import PipelineEvent
from mri.observability.metrics import aggregate, freshness, run_outcome, stage_summaries


def event(
    run_id: str,
    timestamp: str,
    status: str = "success",
    *,
    stage: str = "validate",
    batch_id: str = "",
    records_read: int | None = None,
    duration_ms: int | None = None,
    source: str = "",
) -> PipelineEvent:
    return PipelineEvent(
        timestamp=timestamp,
        run_id=run_id,
        pipeline="ingestion",
        stage=stage,
        status=status,
        batch_id=batch_id,
        records_read=records_read,
        duration_ms=duration_ms,
        source=source,
    )


def test_run_outcome_failed_when_any_event_failed() -> None:
    events = [
        event("r1", "2026-10-09T12:00:00Z", "started"),
        event("r1", "2026-10-09T12:05:00Z", "failed"),
    ]
    assert run_outcome(events) == "failed"


def test_run_outcome_success_only_from_terminal_last_event() -> None:
    assert (
        run_outcome(
            [
                event("r1", "2026-10-09T12:00:00Z", "started"),
                event("r1", "2026-10-09T12:05:00Z", "success"),
            ]
        )
        == "success"
    )
    # last event not a terminal status => crashed => failed
    assert run_outcome([event("r1", "2026-10-09T12:00:00Z", "started")]) == "failed"
    assert run_outcome([]) == "failed"


def test_run_outcome_orders_by_timestamp_not_file_order() -> None:
    events = [
        event("r1", "2026-10-09T12:05:00Z", "success"),
        event("r1", "2026-10-09T12:00:00Z", "started"),
    ]
    assert run_outcome(events) == "success"


def test_stage_summaries_rows_come_from_granular_batch_events() -> None:
    events = [
        event("r1", "2026-10-09T12:00:00Z", "started"),
        event("r1", "2026-10-09T12:01:00Z", "success", batch_id="sales", records_read=20),
        event("r1", "2026-10-09T12:01:00Z", "success", records_read=20, duration_ms=60),
        event("r2", "2026-10-09T12:02:00Z", "started"),
        event("r2", "2026-10-09T12:03:00Z", "success", batch_id="sales", records_read=30),
    ]
    summary = stage_summaries(events)[0]
    assert summary.runs == 2
    assert summary.succeeded == 2
    assert summary.failed == 0
    assert summary.rows_read == 50  # granular only, no double count
    assert summary.rows_rejected == 0


def test_stage_summaries_duration_prefers_explicit_summary() -> None:
    events = [
        event("r1", "2026-10-09T12:00:00Z", "started"),
        event("r1", "2026-10-09T12:00:59Z", "success", duration_ms=40),
    ]
    summary = stage_summaries(events)[0]
    assert summary.duration_ms_avg == 40.0
    assert summary.duration_ms_p95 == 40.0


def test_stage_summaries_duration_falls_back_to_timestamp_span() -> None:
    events = [
        event("r1", "2026-10-09T12:00:00Z", "started"),
        event("r1", "2026-10-09T12:00:02.500000Z", "success"),
    ]
    summary = stage_summaries(events)[0]
    assert summary.duration_ms_avg == 2500.0


def test_freshness_tracks_latest_success_per_source() -> None:
    now = datetime(2026, 10, 9, 12, 10, tzinfo=UTC)
    events = [
        event("r1", "2026-10-09T12:00:00Z", "success", source="sales"),
        event("r1", "2026-10-09T12:01:00Z", "success", source="fx"),
        event("r1", "2026-10-09T12:02:00Z", "failed", source="sales"),  # ignored
        event("r2", "2026-10-09T12:05:00Z", "success", source="sales"),
    ]
    result = {entry["source"]: entry["staleness_seconds"] for entry in freshness(events, now=now)}
    assert result["sales"] == 300
    assert result["fx"] == 540


def test_aggregate_exposes_overall_stages_freshness() -> None:
    events = [event("r1", "2026-10-09T12:00:00Z", "success", batch_id="sales", records_read=9)]
    payload = aggregate(events)
    assert payload["overall"]["runs"] == 1
    assert payload["overall"]["success_rate"] == 100.0
    assert [stage["rows_read"] for stage in payload["stages"]] == [9]
    assert payload["freshness"] == []
