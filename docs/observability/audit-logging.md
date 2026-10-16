# Audit logging and pipeline metrics (Phase 16)

One canonical event schema, one append-only JSONL event log, one PostgreSQL
`ops` mirror, and one SQL/offline metrics surface — derived from that single
source. There is deliberately **no second bespoke audit system**: every stage
of the pipeline (ingestion, Bronze, Silver, warehouse, dbt, Airflow) emits the
same structured event through the same entry point.

- Reference implementation: `src/mri/observability/`
- Decision record: [`ADR-013-observability.md`](../decisions/ADR-013-observability.md)
- CLI entry point: `python -m apps.observability`
- Completion report: [`../reports/p16.md`](../reports/p16.md)

---

## 1. The canonical event

Every operation is one JSON object with a fixed field order (`FIELDS` in
`src/mri/observability/events.py`). The same names and meanings are used in the
JSONL file, the console JSON lines, the `ops.pipeline_event` table and the
`ops` views — so a single event can be located identically in every sink.

| Field | Meaning |
|-------|---------|
| `timestamp` | UTC ISO 8601, microsecond precision (`2026-10-09T12:04:30.123456Z`) |
| `run_id` | application run identity, shared with layer manifests and `audit.load_audit` (format `YYYYMMDDTHHMMSSZ-<8hex>`; external runs prefix `ext-`, dbt runs `dbt-`, Airflow hooks `airflow-`) |
| `dag_run_id` | Airflow DAG run identity (`{{ dag_run.run_id }}`), empty for ad-hoc runs |
| `pipeline` | `ingestion`, `bronze`, `silver`, `warehouse`, `dbt`, or `airflow` |
| `stage` | stage name within the pipeline (`validate`, `ingest`, `external`, `bronze`, `silver`, `warehouse`, `dbt`, `dag_run`) |
| `source` | data source / contract id (`sales`, `weather`, `fx`, …) |
| `batch_id` | content-addressed batch identity (raw filename stem, e.g. `sales__0123abcd`; a warehouse table filter is used as the batch key at the warehouse stage) |
| `status` | `started`, `success`, `failed`, or a stage-specific state (`already_*`, `skipped`, `planned`, …) |
| `duration_ms` | whole milliseconds (CLI-measured where available) |
| `records_read` | rows read by this operation |
| `records_written` | rows written by this operation |
| `records_rejected` | rows rejected by this operation |
| `error_code` | stable machine code (e.g. `VALIDATION_FAILED`, `INGEST_REJECTED`, `EXTERNAL_FAILED`, `BRONZE_FAILED`, `SILVER_LOAD_FAILED`, `WAREHOUSE_UNBALANCED`, `DBT_FAILED`) — empty on success |
| `message` | short human-readable line |
| `payload` | stage-specific structured detail (sha256, partitions, quality counts, timing, …) |

### No secrets, ever

Credentials must never reach an event. `redact()` masks payload keys whose name
smells like a credential (`password`, `passwd`, `secret`, `token`,
`authorization`, `api_key`, `access_key`) anywhere in the nested payload, and
the CLIs never pass credentials to `emit()` in the first place.

## 2. Where events are emitted

Events are emitted at the **application (CLI) boundary**, not deep inside the
library pipelines, so the library behaviour — and its 389 unit tests — is
untouched. Each command writes a `started` run event, granular per-batch events
while work happens, and a terminal summary event.

| Command | Events |
|---------|--------|
| `apps.ingestion validate` | started + one per file (success/failed, `VALIDATION_FAILED`) + summary |
| `apps.ingestion ingest` | started (run id generated before the run) + one per file (`INGEST_REJECTED`) + summary with counts |
| `apps.ingestion external` | started + one per request (`EXTERNAL_FAILED`) + summary with counts |
| `apps.ingestion bronze` | started + one per batch (`BRONZE_FAILED`) + quality summary |
| `apps.ingestion load` | started + one per promoted batch (`SILVER_LOAD_FAILED`) + summary |
| `apps.warehouse build` | started + one per reconciled table (`WAREHOUSE_UNBALANCED`) + summary |
| `apps.dbt …` | started + DAG-level summary + one per model parsed from `dbt/target/run_results.json` (`DBT_MODEL_FAILED`) |
| `apps.observability record` | one manually/injected event (the Airflow DAG hook) |

The same `run_id` value the pipelines already generate for manifests is reused
— so the event log threads onto the existing layer manifests and load-audit
registry, and the `run_id`/`batch_id` columns in `audit.load_audit` join
directly onto `ops.pipeline_event`.

## 3. Correlation model

- **`dag_run_id`** connects every stage of one orchestrated run. The Airflow
  DAG prepends `--dag-run-id "{{ dag_run.run_id }}"` to each task command (a
  final `audit_airflow_run` task writes the DAG-level bookend event); the CLIs
  propagate it into every emitted event.
- **`run_id`** identifies one application run of one pipeline (generated once,
  before the work starts, and threaded into the pipeline so the started event
  is *not* lost when a run crashes).
- **`batch_id`** threads data lineage across stages for one file/batch
  (raw filename stem → Bronze batch → Silver batch → warehouse table subset).

A full trace for one DAG run shows, in order: the ingestion run(s), Bronze
run(s), Silver load run(s), warehouse build run(s), dbt run(s), and the DAG
run event — each referencing the same `dag_run_id` with their own `run_id`s and
chained `batch_id`s.

## 4. Sinks

1. **JSONL event log** — `reports/observability/events.jsonl` (configurable via
   `Settings.observability_dir` / `MRI_OBSERVABILITY_DIR` / `--observability-dir`).
   Append-only, plain text, works offline and in dry runs; `reports/` is
   gitignored, so the log is an operational artifact, never source.
2. **PostgreSQL `ops` schema** — `apps.observability sync` creates
   `ops.pipeline_event`, its indexes and the metric views, then bulk-loads the
   log idempotently (`INSERT … ON CONFLICT (event_hash) DO NOTHING`, keyed on a
   stable content hash). Requires `.env` PostgreSQL credentials.
3. **Console JSON** — opt-in via `MRI_JSON_LOG_CONSOLE=1`. Lines go to
   **stderr**, so stdout (`--json` manifests) is never polluted.

## 5. Metrics semantics (offline CLI == SQL views)

`apps.observability metrics` and the `ops` views compute identical numbers.

- **Run outcome** (`run_outcome` / `v_run_facts.failed`): a run is `failed` if
  *any* of its events failed; otherwise it is `success` only when the
  chronologically last event status is `success`. A run whose last event is not
  terminal is treated as `failed` — **crashes surface in the failure rate**.
- **Row counters**: summed **only over granular events** (those carrying a
  `batch_id`), so a `started`/summary event never double-counts. This is why
  the summary event must not carry the run-total row counts.
- **Duration**: the explicit `duration_ms` summary when a run completed;
  otherwise the span between the first and last stored event (so partial runs
  still produce a non-zero figure).
- **Freshness** (`v_freshness`): per source, the age of the newest successful
  event (`now() − last_successful_at`).

## 6. Inspection commands

```text
python -m apps.observability metrics                 # offline rollups
python -m apps.observability --json metrics          # machine-readable
python -m apps.observability trace --dag-run-id dag__nightly_2026_10_09
python -m apps.observability trace --batch-id sales__0123abcd
python -m apps.observability trace --run-id 20261009T120000Z-abcdef01
python -m apps.observability record --dag-run-id x --pipeline airflow \
    --stage dag_run --status success --message "DAG run completed"
python -m apps.observability sync                     # JSONL -> ops schema (needs .env)
```

`trace` requires exactly one selector (`--run-id`, `--dag-run-id` or
`--batch-id`) and prints the events ordered by timestamp, exit code 1 when
nothing matches. `record` is the Airflow hook used by the DAG's final
`audit_airflow_run` task.

## 7. Relationship to the existing audit registry

The Silver layer already records every load in `audit.load_audit` (run_id,
batch_id, source, target_table, timestamps, status, row counts, error message,
unique `(batch_id, target_table)` for successful loads). That stays as the
**batch-level registry of record**; `ops.pipeline_event` is the **run/event
level view covering every stage**. The two join on `run_id` and `batch_id`, and
no schema migration is required.

## 8. Airflow wiring

`airflow/dags/morocco_retail_pipeline.py` (Phase 16, 8 tasks):

- every task gets `--dag-run-id "{{ dag_run.run_id }}"` prepended to its
  command;
- a final `audit_airflow_run` task runs `apps.observability record
  --dag-run-id "{{ dag_run.run_id }}" --pipeline airflow --stage dag_run
  --status success` after `reconcile`;
- `tests/airflow/validate_dag.py` now asserts that **every** Bash task carries
  `--dag-run-id` (check `dag_run_id_correlated`).

See [`docs/orchestration/airflow.md`](../orchestration/airflow.md).