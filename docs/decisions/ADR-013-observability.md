# ADR-013 — Unified audit logging and pipeline metrics (one event, one log)

- **Status:** Accepted
- **Date:** 2026-10-09
- **Phase:** 16 — Audit logging and pipeline metrics
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Every layer of the platform leaves audit traces, but there was **no unified
view**: ingestion/bronze/external wrote per-run JSON manifests, Silver wrote
`audit.load_audit`, warehouse/dbt wrote nothing machine-readable, and — worse —
**nothing correlated the stages of one orchestrated DAG run**. Answering
"what happened for the 2026-10-09 04:00 DAG run, and did it fail?" required
grepping several different files by hand.

The phase also demanded "reliable metrics" (success/failure rates per stage,
durations, row counts and freshness) without building a second bespoke store
that could drift from reality, and it explicitly forbade disturbing the 389
unit tests of the existing pipeline behaviour.

## Decision

1. **One canonical event schema (`PipelineEvent`) emitted at the application
   (CLI) boundary.** The library pipelines are untouched; each CLI command
   writes a `started` run event (run id generated **before** the run and
   threaded through, so a crashed run still leaves a marker), granular
   per-batch events, and a terminal summary event. See
   [`docs/observability/audit-logging.md`](../observability/audit-logging.md).
2. **One append-only JSONL log is the canonical offline sink**
   (`reports/observability/events.jsonl`, gitignored). It works with no
   database, is cheap to tail, and is bulk-loadable into PostgreSQL.
3. **PostgreSQL `ops` is a mirror, not a second source of truth.** `apps.
   observability sync` creates `ops.pipeline_event` (unique `event_hash` for
   idempotent `ON CONFLICT DO NOTHING`) plus the three metric views
   `v_run_facts`, `v_pipeline_metrics`, `v_freshness` — `CREATE OR REPLACE`,
   so re-syncs never drop. Offline metrics and the SQL views share identical
   definitions.
4. **Correlation is explicit at three granularities:** `dag_run_id` (one
   orchestrated run), `run_id` (one application run, reused from existing
   manifests and `audit.load_audit`), `batch_id` (data lineage for one
   file/batch across stages).
5. **Metrics semantics are crash-honest:** a run with any failed event is
   failed; otherwise it is success only when the last event is terminal, so
   partial/crashed runs are counted as failures. Row counters sum only
   granular (batch-bearing) events. Duration prefers the measured
   `duration_ms`, falling back to the stored timestamp span.
6. **No secrets.** `redact()` masks credential-looking payload keys, and the
   CLIs never pass credentials to `emit()` in the first place.
7. **Console JSON logging is opt-in** (`MRI_JSON_LOG_CONSOLE=1`) and always
   goes to **stderr**, keeping `--json` stdout clean for machine consumers.
8. **Airflow correlates by construction:** every DAG task receives
   `--dag-run-id "{{ dag_run.run_id }}"` and a final `audit_airflow_run` task
   writes the DAG-level bookend event; the in-container DAG validator asserts
   that every Bash task carries the flag (`dag_run_id_correlated`).

## Consequences

- **Positive.** One way to trace and audit any run (`apps.observability trace`)
  and to get metrics offline or in SQL; DAG-run-to-lineage correlation;
  crash detection in failure rates; idempotent PostgreSQL mirror; additive —
  existing manifests, `audit.load_audit` and all 389 unit tests keep working.
- **Cost.** A fixed event vocabulary to learn and a JSONL file to clean up /
  rotate in long-running deployments (out of scope here).
- **Row-counter nuance.** Because summaries never carry row counts, "rows
  read/written per stage" is the sum over granular events; each row is counted
  once per stage in which a batch event exists.
- Slight redundancy with `audit.load_audit` is intentional: the audit table
  remains the batch-level registry of record, joined to `ops.pipeline_event`
  by `run_id`/`batch_id`.