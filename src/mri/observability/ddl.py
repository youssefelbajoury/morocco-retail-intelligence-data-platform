"""PostgreSQL ``ops`` schema: the queryable mirror of the JSONL event log.

``apps.observability sync`` creates this schema and bulk-loads the event log
into ``ops.pipeline_event`` (idempotent via the unique ``event_hash``). The
``v_run_facts`` / ``v_pipeline_metrics`` / ``v_freshness`` views are the
"reliable metrics" query surface; they are ``CREATE OR REPLACE VIEW`` so
re-syncing never needs a drop. The existing ``audit.load_audit`` table keeps
its batch-level role in the same unified model: its ``run_id`` / ``batch_id``
values join directly onto ``ops.pipeline_event``.
"""

from __future__ import annotations

from dataclasses import dataclass

OPS_SCHEMA = "ops"
EVENT_TABLE = "pipeline_event"

V_RUN_FACTS = "v_run_facts"
V_PIPELINE_METRICS = "v_pipeline_metrics"
V_FRESHNESS = "v_freshness"

VIEWS: tuple[str, ...] = (V_RUN_FACTS, V_PIPELINE_METRICS, V_FRESHNESS)


@dataclass(frozen=True)
class EventColumn:
    name: str
    definition: str


# One entry per canonical event field (payload is stored as JSONB).
EVENT_TABLE_COLUMNS: tuple[EventColumn, ...] = (
    EventColumn("event_id", "BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY"),
    EventColumn("event_hash", "TEXT NOT NULL"),
    EventColumn("timestamp", "TIMESTAMPTZ NOT NULL"),
    EventColumn("run_id", "TEXT NOT NULL"),
    EventColumn("dag_run_id", "TEXT NOT NULL DEFAULT ''"),
    EventColumn("pipeline", "TEXT NOT NULL"),
    EventColumn("stage", "TEXT NOT NULL"),
    EventColumn("source", "TEXT NOT NULL DEFAULT ''"),
    EventColumn("batch_id", "TEXT NOT NULL DEFAULT ''"),
    EventColumn("status", "TEXT NOT NULL"),
    EventColumn("duration_ms", "BIGINT"),
    EventColumn("records_read", "BIGINT"),
    EventColumn("records_written", "BIGINT"),
    EventColumn("records_rejected", "BIGINT"),
    EventColumn("error_code", "TEXT NOT NULL DEFAULT ''"),
    EventColumn("message", "TEXT NOT NULL DEFAULT ''"),
    EventColumn("payload", "JSONB NOT NULL DEFAULT '{}'::jsonb"),
)


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def qualified(schema: str, name: str) -> str:
    return f"{ident(schema)}.{ident(name)}"


def render_ops_ddl(schema: str = OPS_SCHEMA) -> list[str]:
    """Idempotent statements that bring the ops schema to its declared state."""
    table = qualified(schema, EVENT_TABLE)
    columns = ", ".join(f"{ident(col.name)} {col.definition}" for col in EVENT_TABLE_COLUMNS)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {ident(schema)}",
        f"CREATE TABLE IF NOT EXISTS {table} ({columns})",
        f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{EVENT_TABLE}_hash ON {table} (event_hash)",
        f"CREATE INDEX IF NOT EXISTS ix_{EVENT_TABLE}_dag_run ON {table} (dag_run_id)",
        f"CREATE INDEX IF NOT EXISTS ix_{EVENT_TABLE}_run ON {table} (run_id)",
        f"CREATE INDEX IF NOT EXISTS ix_{EVENT_TABLE}_pipeline "
        f"ON {table} (pipeline, stage, timestamp)",
        f"CREATE INDEX IF NOT EXISTS ix_{EVENT_TABLE}_batch ON {table} (batch_id)",
        *(_view_statements(schema)),
    ]


def _view_statements(schema: str) -> list[str]:
    table = qualified(schema, EVENT_TABLE)
    run_facts = qualified(schema, V_RUN_FACTS)
    pipeline_metrics = qualified(schema, V_PIPELINE_METRICS)
    freshness = qualified(schema, V_FRESHNESS)
    ddl = [
        f"""CREATE OR REPLACE VIEW {run_facts} AS
SELECT
  run_id,
  max(dag_run_id) AS dag_run_id,
  max(pipeline) AS pipeline,
  max(stage) AS stage,
  min(timestamp) AS started_at,
  max(timestamp) AS finished_at,
  COALESCE(
    max(duration_ms),
    round(extract(epoch FROM (max(timestamp) - min(timestamp))) * 1000)::bigint
  ) AS duration_ms,
  COALESCE(sum(records_read) FILTER (WHERE batch_id <> ''), 0)::bigint AS rows_read,
  COALESCE(sum(records_written) FILTER (WHERE batch_id <> ''), 0)::bigint AS rows_written,
  COALESCE(sum(records_rejected) FILTER (WHERE batch_id <> ''), 0)::bigint AS rows_rejected,
  max(error_code) FILTER (WHERE status = 'failed') AS last_error_code,
  (
    bool_or(status = 'failed')
    OR (array_agg(status ORDER BY timestamp, event_id))
       [cardinality(array_agg(status ORDER BY timestamp, event_id))] <> 'success'
  ) AS failed,
  (
    NOT bool_or(status = 'failed')
    AND (array_agg(status ORDER BY timestamp, event_id))
        [cardinality(array_agg(status ORDER BY timestamp, event_id))] = 'success'
  ) AS succeeded
FROM {table}
GROUP BY run_id""",
        f"""CREATE OR REPLACE VIEW {pipeline_metrics} AS
SELECT
  pipeline,
  stage,
  count(*) AS runs,
  count(*) FILTER (WHERE succeeded) AS succeeded,
  count(*) FILTER (WHERE failed) AS failed,
  round(100.0 * count(*) FILTER (WHERE NOT failed) / greatest(count(*), 1), 2) AS success_rate,
  round(100.0 * count(*) FILTER (WHERE failed) / greatest(count(*), 1), 2) AS failure_rate,
  sum(rows_read) AS rows_read,
  sum(rows_written) AS rows_written,
  sum(rows_rejected) AS rows_rejected,
  round(avg(duration_ms), 1) AS duration_ms_avg,
  round(percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)::numeric, 1) AS duration_ms_p95
FROM {run_facts}
GROUP BY pipeline, stage""",
        f"""CREATE OR REPLACE VIEW {freshness} AS
SELECT
  source,
  max(timestamp) AS last_successful_at,
  round(extract(epoch FROM (now() - max(timestamp))))::bigint AS staleness_seconds
FROM {table}
WHERE status = 'success' AND source <> ''
GROUP BY source""",
    ]
    return ddl


__all__ = [
    "EVENT_TABLE",
    "EVENT_TABLE_COLUMNS",
    "OPS_SCHEMA",
    "VIEWS",
    "ident",
    "qualified",
    "render_ops_ddl",
]
