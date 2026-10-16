# Prometheus + Grafana observability (Phase 19)

The platform publishes operational metrics from its **existing audit surface** —
the `ops` views (`v_pipeline_metrics`, `v_run_facts`, `v_freshness`),
`ops.pipeline_event`, PostgreSQL health statistics and the Airflow metadata
database. A tiny exporter translates those into Prometheus text and Prometheus
stores + serves the series, while Grafana displays them on four version-controlled
dashboards.

The monitoring stack is **optional**: it runs under the `observability` compose
profile and is never required by the ingestion pipeline.

## Architecture

```
pipeline runs ──► ops.{pipeline_event, v_run_facts, v_pipeline_metrics, v_freshness}   (PostgreSQL `retail`)
Airflow runs ──► dag_run / task_instance                                              (PostgreSQL `airflow` metadata)
        │
        ▼
 mri-metrics exporter  (:9245 /metrics)   ── stdlib HTTP, zero new Python deps
        │                         ▲
        ▼                         │ scrape every 30s
   Prometheus (v3.13.4)  ◄────────┘
        │
        ▼
   Grafana (13.2.3) ◄── version-controlled provisioning + 4 dashboards
```

## Services

| Container | Image | Host port | Profile |
| --- | --- | --- | --- |
| `mri-metrics-exporter` | `mri-metrics:0.1.0` (built from `Dockerfile.metrics`) | 9245 | `observability` |
| `mri-prometheus` | `prom/prometheus:v3.13.4` | 9090 | `observability` |
| `mri-grafana` | `grafana/grafana:13.2.3` | 3000 | `observability` |

All images are pinned to exact, verified tags (never `latest`). Prometheus keeps
its time series in a named volume with **15 days OR 1 GiB retention**, whichever
is hit first, so the optional stack stays small (≈0.5 GiB RAM total).

## Start / stop

Everything runs on the shared `mri-net` network, so the base stack must be up
first (PostgreSQL and MinIO; Airflow only if you want Airflow metrics).

```bash
# Preview/validate
make obs-config
make obs-validate

# Start the monitoring stack (base stack must already be running)
make obs-up

# Status, logs, one-shot scrape
make obs-ps
make obs-logs
make obs-metrics

# Stop (keeps Prometheus/Grafana volumes), or stop and wipe data
make obs-down
make obs-down-v
```

Equivalently, without `make`:

```bash
docker compose --env-file .env \
  -f infrastructure/docker/docker-compose.yml \
  -f infrastructure/docker/docker-compose.observability.yml \
  --profile observability up -d
```

Enable all three services at once with the full stack command in
`docker-compose.observability.yml`. The exporter can also be run on the host
without containers:

```bash
uv run python -m apps.metrics            # serve on :9245
uv run python -m apps.metrics --once      # print one scrape and exit
```

## Interfaces

- **Exporter** : `http://localhost:9245/metrics` (Prometheus text), `/healthz`.
- **Prometheus** : `http://localhost:9090` — Targets page shows `mri-metrics`
  and `prometheus` jobs; Alerts page shows the rule evaluations.
- **Grafana** : `http://localhost:3000` (admin credentials come from
  `GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`, defaults `admin`/`admin`).

## The exporter

`src/mri/metrics/` (module `mri.metrics`) is a self-contained, **zero new
dependency** implementation: `model.py` renders the Prometheus text format,
`sources.py` holds SQL + pure row→metric mappers, `exporter.py` runs the scrapes,
`server.py` serves them with the stdlib HTTP server, and `config.py` resolves
connection settings from the environment or the repository `.env`.

Design rules:

1. **Existing sources only.** Nothing here writes to the database or adds its
   own tracking; it is a read-only translation of the audit model (ADR-013) and
   the Airflow metadata. There is no second source of truth.
2. **Bounded labels.** Only low-cardinality dimensions are ever exposed:
   `pipeline`, `stage`, `source`, `table`, `side`, `state`, `dag_id`, `datname`.
   Run ids, batch ids, filenames and customer/product ids never appear as labels.
3. **Fail-safe.** If a source cannot be queried the exporter emits **no** domain
   metrics for it and sets `mri_exporter_last_scrape_success` to `0` (plus an
   incrementing `mri_exporter_scrape_errors_total`). A stale or partial value can
   therefore never masquerade as a fresh one — same philosophy as
   `postgres_exporter`'s `pg_up`.

The exporter queries `mri_postgres_*` from `pg_stat_database` /
`pg_stat_activity` (health and capacity of the database *the exporter itself
uses*), so "PostgreSQL is healthy" is observed directly rather than assumed.

## Scrape configuration

`monitoring/prometheus/prometheus.yml` scrapes:

- `prometheus` (`localhost:9090`) — self-monitoring.
- `mri-metrics` (`metrics-exporter:9245`) — the project exporter.

Intervals are tuned for batch workloads (`scrape_interval: 30s`): the underlying
data only changes when a pipeline stage runs, so a faster cadence would only
waste storage. Alert rules live in
`monitoring/prometheus/rules/mri.rules.yml` and are conservative (long `for:`)
for the same reason.

## Grafana

Grafana is provisioned entirely from version-controlled files
(`monitoring/grafana/provisioning/`): the datasource points at
`http://prometheus:9090` with a fixed UID (`mri-prometheus`), and the dashboard
provider loads the four dashboards into the `MRI` folder. Nothing needs to be
clicked in the UI after first start.

| Dashboard | Purpose |
| --- | --- |
| MRI — Pipeline Overview | runs, failures, durations, last-run health per stage |
| MRI — Data Quality & Reconciliation | warehouse balance, silver vs warehouse rows, rejected rows |
| MRI — Airflow / Task Health | DAG runs, task-instance outcomes, last DAG run age |
| MRI — Data Freshness & Throughput | source staleness, rows read/written, DB size, scrape latency |

Every panel references a metric the exporter actually emits — this is enforced
by the unit test `test_metrics_stack.py::test_dashboards_reference_only_known_metrics`,
so a "fabricated panel" regression is impossible.

## Airflow metrics (optional)

`mri_airflow_*` and `mri_airflow_up` are emitted only when the exporter has an
`AIRFLOW_POSTGRES_*` block configured — the same variables the Airflow overlay
uses. Inside Compose the exporter reaches the metadata DB at
`airflow-postgres:5432`; on the host it defaults to `localhost:5434`. When
unset the Airflow dashboard simply stays empty; the rest of the stack is
unaffected.

## Limitations

- Metrics are **batch-scoped**: granular events are reported per stage, not per
  run, keeping cardinality bounded. Per-run detail stays in the audit log.
- `mri_pipeline_*` rollups reflect the most recent `apps.observability sync`;
  a pipeline run is visible shortly after `make obs-metrics`/`python -m
  apps.observability sync` loads its events into `ops`.
- Prometheus evaluates rules against whatever is in its local TSDB (15d/1 GiB).
- Airflow metrics depend on the exporter being configured with the Airflow
  metadata credentials.

## Validation

- `make obs-validate` (or CI) runs `promtool check config`, validating the
  scrape config and all rule files.
- `test_metrics_stack.py` asserts: exporter targets (`metrics-exporter:9245`),
  bounded retention flags, the four dashboards, stable UIDs, and that every
  alert/dashboard metric name is produced by the exporter.
- `tests/integration/test_metrics_postgres.py` seeds a uniquely-tagged set of
  events and asserts the exporter's output reflects them against a real
  PostgreSQL `ops` schema.

## Troubleshooting

- **Grafana not reachable on host port 3000.** Check for a conflicting host
  service (`netstat -ano | findstr :3000`). This happened on a developer
  machine where a native `postgres` was bound to 0.0.0.0:3000. Set a different
  port: `GRAFANA_PORT=3001` in `.env` and restart the stack (`make obs-down`,
  `make obs-up`). The container health (-healthy flag) is unaffected.
- **Orphan containers warning at `obs-up`.** `docker compose` flags the running
  Airflow services as orphans when the observability profile is started without
  the Airflow compose file. Harmless: all services share `mri-net`, and the
  exporter reaches `airflow-postgres:5432` as long as the
  `AIRFLOW_POSTGRES_{DB,USER,PASSWORD}` credentials are supplied.
- **No `mri_airflow_*` metrics.** The exporter omits Airflow series (and sets
  `mri_airflow_up 0`) when credentials are missing; supply them when starting
  the stack.
- **Dashboard panels empty.** Run `make obs-metrics` and confirm the underlying
  series exist; metrics are batch-scoped and visible after the corresponding
  `apps.observability sync`.