"""Canonical structured events for the unified operational audit model.

Every pipeline operation — across ingestion, Bronze, Silver, warehouse, dbt
and Airflow — emits one :class:`PipelineEvent`. The schema is intentionally
fixed: the same field names appear in the JSONL event log
(``reports/observability/events.jsonl``), in the ``ops.pipeline_event``
PostgreSQL table and in the JSON console log lines, so a single event can be
located identically in every sink.

Never put secrets in an event: ``redact`` masks payload keys whose name smells
like a credential (passwords, tokens, API keys, ...) before serialization, and
the CLIs never pass credentials to ``emit`` in the first place.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

FIELDS: tuple[str, ...] = (
    "timestamp",
    "run_id",
    "dag_run_id",
    "pipeline",
    "stage",
    "source",
    "batch_id",
    "status",
    "duration_ms",
    "records_read",
    "records_written",
    "records_rejected",
    "error_code",
    "message",
    "payload",
)

STATUS_STARTED = "started"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"

SENSITIVE_HINTS: tuple[str, ...] = (
    "password",
    "passwd",
    "secret",
    "token",
    "authorization",
    "api_key",
    "access_key",
)

REDACTED = "***"


def now_timestamp() -> str:
    """Current UTC time in the canonical ISO format used across the audit model."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _is_sensitive(key: str) -> bool:
    lowered = key.lower()
    return any(hint in lowered for hint in SENSITIVE_HINTS)


def redact(value: Any) -> Any:
    """Mask credential-looking keys anywhere in a nested payload.

    Keys are checked against :data:`SENSITIVE_HINTS`; matched values become
    ``"***"``. Everything else is copied as-is (nested mappings and lists are
    recursed into).
    """
    if isinstance(value, Mapping):
        return {str(k): REDACTED if _is_sensitive(str(k)) else redact(v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return [redact(item) for item in value]
    return value


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


@dataclass(frozen=True)
class PipelineEvent:
    """One structured operation; the single unit of the audit model."""

    timestamp: str
    run_id: str
    pipeline: str
    stage: str
    status: str
    dag_run_id: str = ""
    source: str = ""
    batch_id: str = ""
    duration_ms: int | None = None
    records_read: int | None = None
    records_written: int | None = None
    records_rejected: int | None = None
    error_code: str = ""
    message: str = ""
    payload: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        """The canonical mapping (fixed key order, payload redacted)."""
        return {
            "timestamp": self.timestamp,
            "run_id": self.run_id,
            "dag_run_id": self.dag_run_id,
            "pipeline": self.pipeline,
            "stage": self.stage,
            "source": self.source,
            "batch_id": self.batch_id,
            "status": self.status,
            "duration_ms": self.duration_ms,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_rejected": self.records_rejected,
            "error_code": self.error_code,
            "message": self.message,
            "payload": redact(dict(self.payload)),
        }

    def to_json(self) -> str:
        """One compact JSON line (JSONL format)."""
        return json.dumps(self.as_dict(), ensure_ascii=False, separators=(",", ":"))

    def event_hash(self) -> str:
        """Stable content hash used to keep ``ops.sync`` idempotent."""
        canonical = json.dumps(
            self.as_dict(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> PipelineEvent:
        """Parse a mapping back into an event, tolerating missing/odd values."""
        payload = data.get("payload") or {}
        if not isinstance(payload, Mapping):
            payload = {}
        return cls(
            timestamp=str(data.get("timestamp") or now_timestamp()),
            run_id=str(data.get("run_id") or ""),
            dag_run_id=str(data.get("dag_run_id") or ""),
            pipeline=str(data.get("pipeline") or ""),
            stage=str(data.get("stage") or ""),
            source=str(data.get("source") or ""),
            batch_id=str(data.get("batch_id") or ""),
            status=str(data.get("status") or ""),
            duration_ms=_as_int(data.get("duration_ms")),
            records_read=_as_int(data.get("records_read")),
            records_written=_as_int(data.get("records_written")),
            records_rejected=_as_int(data.get("records_rejected")),
            error_code=str(data.get("error_code") or ""),
            message=str(data.get("message") or ""),
            payload=dict(payload),
        )

    @classmethod
    def from_json_line(cls, line: str) -> PipelineEvent:
        data = json.loads(line)
        if not isinstance(data, Mapping):
            raise ValueError("event JSON line is not an object")
        return cls.from_dict(data)


__all__ = [
    "FIELDS",
    "REDACTED",
    "SENSITIVE_HINTS",
    "STATUS_FAILED",
    "STATUS_STARTED",
    "STATUS_SUCCESS",
    "PipelineEvent",
    "now_timestamp",
    "redact",
]
