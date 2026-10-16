"""Phase 16: unit tests for the append-only JSONL event log."""

from __future__ import annotations

from pathlib import Path

import pytest

from mri.observability.events import PipelineEvent
from mri.observability.store import EVENTS_FILENAME, EventLog, EventLogError


def _event(run_id: str, status: str = "success") -> PipelineEvent:
    return PipelineEvent(
        timestamp="2026-10-09T12:00:00.000000Z",
        run_id=run_id,
        pipeline="ingestion",
        stage="validate",
        status=status,
        batch_id="sales",
    )


def test_resolve_points_inside_the_directory(tmp_path: Path) -> None:
    log = EventLog.resolve(tmp_path)
    assert log.path == tmp_path / EVENTS_FILENAME


def test_append_creates_parents_and_file(tmp_path: Path) -> None:
    log = EventLog.resolve(tmp_path / "a" / "b")
    assert not log.path.exists()
    log.append(_event("r1"))
    assert log.path.is_file()


def test_count_lines_tracks_appends(tmp_path: Path) -> None:
    log = EventLog.resolve(tmp_path)
    assert log.count_lines() == 0
    log.append(_event("r1"))
    log.append(_event("r2"))
    assert log.count_lines() == 2


def test_read_events_round_trips(tmp_path: Path) -> None:
    log = EventLog.resolve(tmp_path)
    log.append(_event("r1"))
    log.append(_event("r2", status="failed"))
    events = log.read_events()
    assert [event.run_id for event in events] == ["r1", "r2"]
    assert events[1].status == "failed"


def test_read_events_skips_corrupt_lines(tmp_path: Path) -> None:
    log = EventLog.resolve(tmp_path)
    log.append(_event("r1"))
    log.path.with_suffix(".tmp").write_text("not-json\n", encoding="utf-8")
    with log.path.open("a", encoding="utf-8") as handle:
        handle.write("not-json\n\n")
    events = log.read_events()
    assert [event.run_id for event in events] == ["r1"]


def test_read_events_returns_empty_for_missing_file(tmp_path: Path) -> None:
    assert EventLog.resolve(tmp_path).read_events() == []


def test_append_raises_on_unwritable_target(tmp_path: Path) -> None:
    lock = tmp_path / "lock"
    lock.mkdir()
    log = EventLog(lock)  # path collides with a directory
    with pytest.raises(EventLogError):
        log.append(_event("r1"))


def test_clear_removes_the_file(tmp_path: Path) -> None:
    log = EventLog.resolve(tmp_path)
    log.append(_event("r1"))
    log.clear()
    assert not log.path.exists()
    log.clear()  # idempotent
