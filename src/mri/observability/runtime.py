"""Process-wide routing for the audit model.

The CLI entry points stamp a *correlation context* once per process
(:func:`set_context`): an optional ``dag_run_id`` (Airflow DAG run identity)
and the event log directory. :func:`emit` then appends every operation to the
JSONL log (always) and to the console (only when ``MRI_JSON_LOG_CONSOLE`` is
on). The same ``run_id`` value that pipelines already generate for manifests
is reused here, so the application run identity is shared with the existing
layer manifests and the ``audit.load_audit`` registry.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mri.observability.events import PipelineEvent, now_timestamp, redact
from mri.observability.logging import console_when_enabled
from mri.observability.store import EventLog

DEFAULT_EVENTS_DIR = Path("reports") / "observability"


@dataclass(frozen=True)
class RuntimeContext:
    """The correlation context shared by every event emitted this process."""

    dag_run_id: str = ""
    events_dir: Path | None = None


_active: RuntimeContext = RuntimeContext()


def set_context(
    *,
    dag_run_id: str | None = None,
    events_dir: str | Path | None = None,
) -> None:
    """Stamp correlation values for the remainder of the process."""
    global _active
    _active = RuntimeContext(
        dag_run_id=dag_run_id if dag_run_id is not None else _active.dag_run_id,
        events_dir=Path(events_dir) if events_dir is not None else _active.events_dir,
    )


def reset_context() -> None:
    global _active
    _active = RuntimeContext()


def get_context() -> RuntimeContext:
    return _active


def event_log() -> EventLog:
    return EventLog.resolve(_active.events_dir or DEFAULT_EVENTS_DIR)


def elapsed_ms(started_perf: float) -> int:
    """Whole milliseconds since a :func:`time.perf_counter` start."""
    return int(round((time.perf_counter() - started_perf) * 1000))


def emit(
    *,
    pipeline: str,
    stage: str,
    run_id: str,
    status: str,
    source: str = "",
    batch_id: str = "",
    dag_run_id: str | None = None,
    duration_ms: int | None = None,
    records_read: int | None = None,
    records_written: int | None = None,
    records_rejected: int | None = None,
    error_code: str = "",
    message: str = "",
    payload: Mapping[str, Any] | None = None,
    timestamp: str | None = None,
) -> PipelineEvent:
    """Persist one operation to the JSONL event log and return it.

    Credentials must never be passed here; the payload is additionally
    passed through :func:`mri.observability.events.redact` as a safety net.
    """
    event = PipelineEvent(
        timestamp=timestamp or now_timestamp(),
        run_id=run_id,
        dag_run_id=dag_run_id if dag_run_id is not None else _active.dag_run_id,
        pipeline=pipeline,
        stage=stage,
        source=source,
        batch_id=batch_id,
        status=status,
        duration_ms=duration_ms,
        records_read=records_read,
        records_written=records_written,
        records_rejected=records_rejected,
        error_code=error_code,
        message=message,
        payload=redact(dict(payload or {})),
    )
    event_log().append(event)
    console_when_enabled(event)
    return event


__all__ = [
    "DEFAULT_EVENTS_DIR",
    "RuntimeContext",
    "elapsed_ms",
    "emit",
    "event_log",
    "get_context",
    "reset_context",
    "set_context",
]
