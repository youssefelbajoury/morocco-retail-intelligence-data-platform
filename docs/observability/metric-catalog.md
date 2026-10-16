# MRI Prometheus metric catalog (Phase 19)

Every metric the exporter (`src/mri/metrics/`) can emit. Type and dimension are
fixed; the help strings below match the emitted `# HELP`. "Upstream" is the
table/view the value is read from.

Labels are strictly bounded (see ADR-015): pipelines/stages/sources/tables are
small enumerations; never run ids, batch ids, filenames or customer/product ids.

## Pipeline execution (`ops.v_pipeline_metrics`)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `mri_pipeline_runs` | gauge | pipeline, stage | total application runs |
| `mri_pipeline_succeeded` | gauge | pipeline, stage | successful runs |
| `mri_pipeline_failed` | gauge | pipeline, stage | failed runs |
| `mri_pipeline_success_rate` | gauge | pipeline, stage | % successful runs |
| `mri_pipeline_rows_read` | gauge | pipeline, stage | records read |
| `mri_pipeline_rows_written` | gauge | pipeline, stage | records written |
| `mri_pipeline_rows_rejected` | gauge | pipeline, stage | records rejected |
| `mri_pipeline_duration_ms_avg` | gauge | pipeline, stage | average run duration (ms) |
| `mri_pipeline_duration_ms_p95` | gauge | pipeline, stage | p95 run duration (ms) |

## Latest-run recency (`ops.v_run_facts`)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `mri_pipeline_last_run_timestamp_seconds` | gauge | pipeline, stage | unix ts of most recent run |
| `mri_pipeline_last_success_timestamp_seconds` | gauge | pipeline, stage | unix ts of most recent success |
| `mri_pipeline_last_run_succeeded` | gauge | pipeline, stage | 1 if latest run succeeded, else 0 |

## Source freshness (`ops.v_freshness`)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `mri_source_freshness_seconds` | gauge | source | age (s) of the newest successful event |
| `mri_source_last_success_timestamp_seconds` | gauge | source | unix ts of the newest successful event |

## Warehouse reconciliation (`ops.pipeline_event`, `warehouse/warehouse`)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `mri_warehouse_reconcile_balanced` | gauge | table | 1 balanced (silver ≡ warehouse), else 0 |
| `mri_warehouse_reconcile_rows` | gauge | table, side (silver\|warehouse) | reconciled row count per side |

## PostgreSQL health & capacity (`pg_stat_database`, `pg_stat_activity`)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `mri_postgres_database_size_bytes` | gauge | datname | on-disk size of current database |
| `mri_postgres_cache_hit_ratio` | gauge | datname | buffer cache hit ratio (0..1) |
| `mri_postgres_deadlocks` | gauge | datname | cumulative deadlocks |
| `mri_postgres_connections` | gauge | state | connections by backend state |
| `mri_postgres_up` | gauge | — | 1 if PostgreSQL reachable, else 0 |

## Airflow orchestration (Airflow metadata DB — emitted when configured)

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `mri_airflow_dag_runs` | gauge | dag_id, state | DAG-run count by DAG and state |
| `mri_airflow_last_dag_run_timestamp_seconds` | gauge | dag_id | unix ts of the most recent run |
| `mri_airflow_task_instances` | gauge | dag_id, state | task-instance count by DAG and state |
| `mri_airflow_up` | gauge | — | 1 if Airflow metadata DB reachable, else 0 |

## Exporter self-diagnostics

| Metric | Type | Labels | Description |
| --- | --- | --- | --- |
| `mri_exporter_build_info` | gauge | version | constant 1 carrying the build version |
| `mri_exporter_last_scrape_success` | gauge | — | 1 full set, 0 degraded (no misleading data) |
| `mri_exporter_scrape_duration_seconds` | gauge | — | wall-clock duration of last scrape |
| `mri_exporter_scrape_errors_total` | counter | source (retail\|airflow) | cumulative query failures by source |

## Endpoints

- Scrape: `GET /metrics` (Prometheus text `version=0.0.4`).
- Liveness: `GET /healthz`, `GET /-/healthy`.
- Index: `GET /`.