# Troubleshooting Runbook

Operational fixes for the most common failures in this local stack. Commands are
PowerShell, run from the repository root. **Never print or paste secrets** — the
values live in your local `.env`.

## Quick triage

```powershell
# container health
docker ps --format "table {{.Names}}\t{{.Status}}"
# compose status per overlay
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml ps
# base-stack smoke test (read-only)
powershell -NoProfile -ExecutionPolicy Bypass -File infrastructure/scripts/smoke-test.ps1
```

## Compose / environment

- **`variable is not set` warnings or blank config** — `.env` is missing or not
  passed. Compose does not auto-detect the root `.env` because the compose files
  live in `infrastructure/docker`; always pass `--env-file .env`.
- **`Recreate` fails on ports** — a native PostgreSQL 16 service commonly owns
  `5432`. This stack publishes PostgreSQL on `5433`; keep that. Free the other
  host ports (`9000`, `9001`, `8080`, `8088`, `9090`, `3001`, `9245`) or edit the
  `*_PORT` keys in `.env`.
- **Data disappeared** — `docker compose down -v` deletes named volumes. Use
  `down` (no `-v`) to keep data.

## MinIO

- **Image pull denied / `no such manifest`** — MinIO community images were
  withdrawn upstream. The stack pins a maintained Chainguard rebuild by digest;
  do not switch back to `minio/minio:*`. See `ADR-001`.
- **Bucket missing** — buckets are created by the ingestion bootstrap; run
  `uv run python -m apps.ingestion ingest --path data/generated/tiny --recursive`.

## Airflow

- **UI not reachable** — check `docker ps` (`mri-airflow-apiserver` healthy), then
  `docker logs mri-airflow-apiserver` / `mri-airflow-scheduler`.
- **Task fails with `Name or service not known`** — container DNS. The compose
  files set `dns: [8.8.8.8, 1.1.1.1]` to bypass Docker Desktop's embedded
  resolver; if it recurs, restart the affected container and re-run.
- **`dbt_build` fails with command-not-found** — the task venv `bin` must be on
  `PATH`; the DAG wraps commands so this is handled. If you changed the DAG,
  verify the `_bash()` PATH prefix.
- **Transient `httpx.ReadTimeout` on the first ingest attempt** — the task
  runner's call to the API server can time out under memory pressure; Airflow's
  automatic retry (`2 × 5 min`) recovers. This is a known, non-fatal transient.
- **DAG not listed** — validate the DagBag:
  `make airflow-validate` or
  `docker compose ... exec -T airflow-scheduler python /opt/airflow/mri/tests/airflow/validate_dag.py`.

## Superset

- **Login fails** — confirm `SUPERSET_ADMIN_USER` / `SUPERSET_ADMIN_PASSWORD` and
  that `superset-init` completed (`docker logs mri-superset-init`).
- **No data / permission denied on queries** — apply the read-only grants once:
  `docker exec -i mri-postgres psql -U retail_app -d retail -f infrastructure/sql/bi-reader-readonly.sql`.
- **Charts empty** — re-seed (`uv run python dashboards/superset/seed.py all`) then
  `uv run python dashboards/superset/verify.py`.
- **`GET /api/v1/chart/{id}/data/` returns "no query context saved"** — expected for
  API-created charts; that endpoint only replays a UI-cached query.

## Monitoring (optional profile)

- **Prometheus target down** — confirm the exporter container is up and the
  `observability` profile was used; `curl http://localhost:9245/metrics` should
  return `mri_*` series.
- **Grafana empty dashboards** — dashboards are provisioned from
  `monitoring/grafana/`; check `monitoring/prometheus/prometheus.yml` scrape
  config and `docker logs mri-grafana`.
- **Validate config** — `make obs-validate` (promtool).

## Python / tests

- **Wrong Python used** — always go through `uv run` (Python 3.12). The host
  default `python` may be a newer, unsupported version.
- **Integration tests skip** — they need the containers up; live external-API
  smoke tests require `MRI_LIVE_SMOKE=1`.
- **dbt tests fail after schema changes** — rebuild with a fresh schema
  (`uv run python -m apps.dbt build --vars '{schema_suffix: _it1}'`) to isolate.

## Resource pressure

The host has ~7.8 GiB RAM with a ~3.7 GiB Docker VM. Start services **in groups**
(base → Airflow → Superset → monitoring) and stop what you do not need. If a
container is killed (OOM), raise Docker Desktop memory or run fewer overlays.
