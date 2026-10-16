"""Reliable metrics derived from the single JSONL event log.

Everything here is a pure function over :class:`PipelineEvent` records, so the
same numbers are available offline (``apps.observability metrics``) and, after
``apps.observability sync``, through the ``ops.v_run_facts`` /
``ops.v_pipeline_metrics`` / ``ops.v_freshness`` SQL views. Deriving metrics
from *one* event log — never from a second bespoke store — is what keeps the
audit model unified.
"""

from __future__ import annotations

import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from typing import Any

from mri.observability.events import PipelineEvent, now_timestamp

FAILED_STATUS = "failed"


@dataclass(frozen=True)
class StageMetrics:
    """Rollups for one (pipeline, stage) pair over all runs in a log."""

    pipeline: str
    stage: str
    runs: int
    succeeded: int
    failed: int
    success_rate: float
    failure_rate: float
    rows_read: int
    rows_written: int
    rows_rejected: int
    duration_ms_avg: float
    duration_ms_p95: float


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def run_outcome(events: Sequence[PipelineEvent]) -> str:
    """Classify one run: ``failed`` on any failure, else the terminal status.

    A run whose last stored event is not a terminal status is treated as
    ``failed`` (crashed / incomplete), so crashes surface in the failure rate.
    """
    ordered = sorted(events, key=lambda event: parse_timestamp(event.timestamp))
    if any(event.status == FAILED_STATUS for event in ordered):
        return FAILED_STATUS
    if not ordered or ordered[-1].status != "success":
        return FAILED_STATUS
    return "success"


def _run_duration_ms(events: Sequence[PipelineEvent]) -> int:
    """Duration of one run: the explicit ``duration_ms`` summary, else the span.

    Run-level events carry the CLI-measured ``duration_ms``; when a run crashed
    before writing its summary we fall back to the time span between the first
    and last stored event so partial runs still produce a nonzero figure.
    """
    if not events:
        return 0
    explicit = [event.duration_ms or 0 for event in events if event.duration_ms]
    if explicit:
        return max(explicit)
    stamps = [parse_timestamp(event.timestamp) for event in events]
    return max(0, int((max(stamps) - min(stamps)).total_seconds() * 1000))


def _percentile(values: Sequence[int], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(quantile * (len(ordered) - 1)))
    return float(ordered[index])


def stage_summaries(events: Sequence[PipelineEvent]) -> list[StageMetrics]:
    """Per (pipeline, stage) rollups: runs, outcomes, rows and durations."""
    groups: dict[tuple[str, str], dict[str, list[PipelineEvent]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for event in events:
        groups[(event.pipeline, event.stage)][event.run_id].append(event)

    summaries: list[StageMetrics] = []
    for (pipeline, stage), runs in sorted(groups.items()):
        granular = [event for run in runs.values() for event in run if event.batch_id]
        rows_read = sum(event.records_read or 0 for event in granular)
        rows_written = sum(event.records_written or 0 for event in granular)
        rows_rejected = sum(event.records_rejected or 0 for event in granular)
        outcomes = [run_outcome(run) for run in runs.values()]
        succeeded = outcomes.count("success")
        failed = outcomes.count(FAILED_STATUS)
        total = max(len(outcomes), 1)
        durations = [_run_duration_ms(run) for run in runs.values()]
        summaries.append(
            StageMetrics(
                pipeline=pipeline,
                stage=stage,
                runs=len(outcomes),
                succeeded=succeeded,
                failed=failed,
                success_rate=round(100.0 * succeeded / total, 2),
                failure_rate=round(100.0 * failed / total, 2),
                rows_read=rows_read,
                rows_written=rows_written,
                rows_rejected=rows_rejected,
                duration_ms_avg=round(statistics.fmean(durations), 1) if durations else 0.0,
                duration_ms_p95=round(_percentile(durations, 0.95), 1),
            )
        )
    return summaries


def freshness(
    events: Sequence[PipelineEvent],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Data freshness: the age of the newest successful event per source."""
    reference = now or datetime.now(UTC)
    latest: dict[str, datetime] = {}
    for event in events:
        if not event.source or event.status != "success":
            continue
        stamp = parse_timestamp(event.timestamp)
        previous = latest.get(event.source)
        if previous is None or stamp > previous:
            latest[event.source] = stamp
    return [
        {
            "source": source,
            "last_successful_at": stamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "staleness_seconds": int((reference - stamp).total_seconds()),
        }
        for source, stamp in sorted(latest.items())
    ]


def aggregate(events: Sequence[PipelineEvent]) -> dict[str, Any]:
    """The full metrics payload the ``metrics`` CLI and tests consume."""
    stages = stage_summaries(events)
    all_runs: dict[str, list[PipelineEvent]] = defaultdict(list)
    for event in events:
        all_runs[event.run_id].append(event)
    outcomes = [run_outcome(run) for run in all_runs.values()]
    total = max(len(outcomes), 1)
    overall: Mapping[str, Any] = {
        "runs": len(outcomes),
        "succeeded": outcomes.count("success"),
        "failed": outcomes.count(FAILED_STATUS),
        "success_rate": round(100.0 * outcomes.count("success") / total, 2),
        "failure_rate": round(100.0 * outcomes.count(FAILED_STATUS) / total, 2),
    }
    return {
        "updated_at": now_timestamp(),
        "overall": dict(overall),
        "stages": [asdict(stage) for stage in stages],
        "freshness": freshness(events),
    }


__all__ = [
    "StageMetrics",
    "aggregate",
    "freshness",
    "parse_timestamp",
    "run_outcome",
    "stage_summaries",
]
