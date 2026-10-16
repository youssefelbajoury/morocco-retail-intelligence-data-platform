"""Phase 16 — structured audit logging and pipeline metrics.

One canonical event schema, one JSONL event log, one ``ops`` PostgreSQL
mirror, metrics derived from that single source. See
``docs/observability/audit-logging.md``.
"""

from mri.observability.events import PipelineEvent, redact
from mri.observability.runtime import emit, reset_context, set_context

__all__ = ["PipelineEvent", "emit", "redact", "reset_context", "set_context"]
