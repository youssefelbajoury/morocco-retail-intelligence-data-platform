# ADR-009 — Apache Airflow 3.3.2 (LocalExecutor) owns orchestration

- **Status:** Accepted
- **Date:** 2026-10-08
- **Phase:** 11 — Apache Airflow orchestration
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phases 00–10 delivered a fully tested pipeline with stable CLI entry points
(`apps.ingestion`, `apps.warehouse`, `apps.dbt`) but **no scheduler**: the
steps only run when a human invokes them. Phase 11 had to add orchestration
without regressing anything, under six constraints:

1. **Host resource ceiling (7.8 GiB).** PostgreSQL and MinIO must keep
   working; whatever is deployed must fit beside them.
2. **No Airflow in the project Python runtime.** The project's environment is
   locked by `uv.lock` and used by 344 host tests; installing Airflow into it
   would drag in pinned-version conflicts and bloat `uv.lock`.
3. **DAGs must stay orchestration-only.** Business logic belongs to `src/mri/`,
   `apps/`, `dbt/` — the DAG coordinates, it does not compute.
4. **Airflow needs its own metadata database**, never the retail warehouse.
5. **Service-to-service addressing** must use Docker service names
   (`postgres`, `minio`), not `localhost`.
6. **The DAG must be provable**: parsed with the *real* Airflow API and
   executed end-to-end at least once, with idempotence demonstrated.

## Decision

1. **Official Apache Airflow 3.3.2 containers, layered on the base compose
   file.** `infrastructure/docker/docker-compose.airflow.yml` adds
   `airflow-init` (one-shot migrations + admin user), `airflow-postgres`
   (dedicated metadata DB, own volume, host port 5434), `airflow-apiserver`
   (UI :8080), `airflow-scheduler`, `airflow-dag-processor` and an on-demand
   `airflow-cli` (`profiles: [debug]`). Both compose files are always passed
   in one project on the shared `mri-net` network.
2. **`LocalExecutor` — the lightest official topology.** No Redis, no Celery
   worker, no triggerer, no flower: the scheduler executes tasks in-process
   (`PARALLELISM=4`). Observed footprint: ~1.11 GiB idle / ~1.31 GiB peak for
   the whole Airflow layer; the base stack was unaffected.
3. **Custom image `mri-airflow:3.3.2` = official image + isolated project
   venv.** `infrastructure/docker/Dockerfile.airflow` layers
   `ghcr.io/astral-sh/uv:0.12.1` onto `apache/airflow:3.3.2-python3.12` and
   runs `uv sync --frozen --no-dev` into `UV_PROJECT_ENVIRONMENT=/opt/venv`.
   Airflow keeps its own pinned Python; tasks run `/opt/venv/bin/python`,
   resolving the project exactly like `uv run` on the host (single source of
   truth: `uv.lock`). Task commands prefix `PATH=/opt/venv/bin` so console
   scripts (`dbt`) resolve — the failure mode observed and fixed during
   acceptance.
4. **One DAG, seven thin `BashOperator` tasks calling stable CLIs.**
   `morocco_retail_pipeline` (file `airflow/dags/morocco_retail_pipeline.py`):
   `ingest_internal` and `ingest_external` in parallel →
   `bronze_internal_external` → `silver_load` → `warehouse_build_reference`
   → `dbt_build` → `reconcile`. All parameters (schedule, retries, paths) are
   environment-driven (`MRI_DAG_SCHEDULE` default `0 4 * * *` UTC,
   `MRI_DAG_RETRIES=2`, retry delay 5 min) with per-task
   `execution_timeout`s, `dagrun_timeout=6h`, `catchup=False`,
   `max_active_runs=1`, and the DAG paused at creation.
5. **Idempotence stays delegated to the existing layers** (content-addressed
   batches, `already_fetched` markers, `load_audit` registry, rebuild
   warehouse, deterministic dbt models). The DAG adds no duplicate-work
   logic of its own — acceptance proved it: a second consecutive run left
   23/24 snapshot tables bit-identical, with only the append-only
   `audit.load_audit` trail growing (+41 `skipped` entries).
6. **Bounded failure semantics.** 2 retries × 5 min, execution timeouts, and
   `upstream_failed` propagation — a failed `ingest_external` (DNS outage)
   correctly left all five downstream tasks `upstream_failed`; nothing
   downstream was reported successful.
7. **DAG tests run against real Airflow *inside* the container.**
   `tests/airflow/validate_dag.py` loads the DAG with `airflow.models.DagBag`
   (task IDs, edges, acyclicity, retries, timeouts, schedule) and emits
   JSON; `tests/integration/test_airflow_dag.py` executes it via
   `docker compose exec -T` from the host and skips cleanly when Docker/the
   stack is absent. The host stays Airflow-free.
8. **Explicit `dns: [8.8.8.8, 1.1.1.1]` for the Airflow services.** During
   acceptance, Docker Desktop's embedded resolver failed every external
   lookup (`Errno -2`) for ~70 minutes, exhausting the app's internal retries
   and the task's 2 retries; the override removes the dependency on the
   embedded resolver.

## Alternatives considered

* **Install Airflow into the project env (uv/pip)** — rejected: violates the
  constraint above, needs official constraints files to avoid downgrades, and
  couples the runtime to test tooling.
* **Host cron / a Makefile loop** — rejected: no UI, no run history, no
  retries/timeouts, no dependency graph; would not satisfy the phase spec.
* **`CeleryExecutor` + Redis (official "basic" topology)** — rejected: two
  extra containers and materially more RAM for a 7-task, single-node batch
  DAG; the official LocalExecutor deployment is sufficient and lighter.
* **Prefect / Dagster / custom scheduler** — rejected: the requirement is
  Apache Airflow with official containers and official version pinning.
* **Duplicate pipeline logic inside DAG tasks (PythonOperator with inline
  code)** — rejected: would violate orchestration-only and make the CLI the
  second-class citizen; tests already gate the CLIs.

## Consequences

* **Positive:** a scheduled, monitored, retrying pipeline with a real UI
  (`http://localhost:8080`), run history, and one-command operation; DAG
  correctness asserted by the host test gate; no changes to `src/`, `apps/`,
  `dbt/`, `uv.lock`, or the host Python environment; existing PostgreSQL/
  MinIO untouched.
* **Negative / operational:** ~1.1 GiB extra RAM (documented, measured);
  six more containers to run; runs are **pinned to the DAG version at trigger
  time** (Airflow 3 DAG versioning — a fix requires a re-parse and a *new*
  run); unpausing on a day whose cron time already passed fires that single
  interval once; external-API tasks depend on container DNS, which is why the
  explicit resolvers and bounded retries exist.
* **Follow-ups:** CI validation of `docker compose config` for the Airflow
  overlay is reserved for Phase 12 (GitHub Actions); upgrading beyond 3.3.2
  should be done by changing the pinned tags in
  `docker-compose.airflow.yml` / `Dockerfile.airflow` only.
