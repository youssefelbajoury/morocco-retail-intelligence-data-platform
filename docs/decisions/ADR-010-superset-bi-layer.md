# ADR-010 — Apache Superset 6.1.0 serves the warehouse as the BI layer

- **Status:** Accepted
- **Date:** 2026-10-08
- **Phase:** 12 — Superset BI layer
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phases 09–10 produced a PostgreSQL star-schema warehouse (`warehouse` schema,
Python build) plus an equivalent dbt analytics layer (`dbt_marts`), and Phase 11
orchestrated both through Airflow. What remained was the **consumption layer**:
executives need interactive dashboards against the facts and dimensions, with
business-level charts, filtering, and zero write access to the warehouse.

Phase 12 had to select and run a BI tool under tight constraints inherited from
Phases 09–11:

1. **Host resource ceiling (7.8 GiB).** PostgreSQL, MinIO and Airflow already
   run locally; any BI tool must fit beside them.
2. **Read-only access.** BI must never be able to mutate warehouse data. A
   dedicated role, not the pipeline role, must own the connection.
3. **No heavyweight infra.** Redis, a dedicated metadata database, message
   queues and worker clusters are overkill for a single-user local deployment.
4. **Reproducible seeding.** Dashboards/charts must be created declaratively
   via the management API (idempotent), not by hand-clicking the UI.
5. **Provable end state.** The seeded layer must be verifiable: object counts,
   layout integrity, chart rendering, and no orphans.

## Considered options

- **Apache Superset (chosen, 6.1.0).** Feature-complete open-source BI, official
  Docker image, mature REST API (`/api/v1/**`) that supports fully
  API-driven seeding, rich native-filter support, and a large chart catalog
  (KPIs, time-series, pies, tables). Costs one container.
- **Metabase.** Great look and feel and simple provisioning, but its
  dashboard/query configuration is less amenable to clean API-driven,
  version-controlled seeding than Superset's.
- **Grafana + SQL/plugin.** Excellent time-series, weaker for the business
  KPIs/tables/pies this platform's deliverables emphasize, and its SQL datasource
  charts are lower-level for a BI audience.
- **Streamlit/Panel app.** Full freedom, zero new services, but user-built
  dashboards rather than an off-the-shelf BI tool; rejected as not "BI".

Superset was already named in the platform's architecture (README architecture
diagram), so the decision cost is mainly installation and seeding.

## Decision

1. **Superset 6.1.0 on a custom, driver-complete image.**
   `mri-superset:6.1.0` extends the official `apache/superset:6.1.0` image with
   `psycopg2-binary==2.9.10` (the official image ships **without** a PostgreSQL
   driver) — see `infrastructure/docker/Dockerfile.superset`. Installed into the
   image's own virtualenv (`/app/.venv`) so the stock entrypoint works unchanged.
2. **Lightest topology.** One `superset-init` one-shot (migrations + admin) and
   one `superset` web server. SQLite metadata in the `superset_home` volume. No
   Redis, no metadata Postgres, no Celery/beat, `SUPERSET_LOAD_EXAMPLES=no`.
   Documented in `docs/bi/superset.md`; layered compose file
   `infrastructure/docker/docker-compose.superset.yml` joins the base stack on
   `mri-net` (warehouse reachable only by service name `postgres`).
3. **Read-only role `superset_bi`.** `infrastructure/sql/bi-reader-readonly.sql`
   creates a `LOGIN` role with `CONNECT`/`USAGE`/`SELECT` on `warehouse`, `audit`
   and `dbt_marts` plus `ALTER DEFAULT PRIVILEGES`; Superset's **Retail
   Warehouse** SQLAlchemy URL uses only this role/project `.env` values.
4. **Declarative, API-driven seeding.** `dashboards/superset/seed.py`:
   database → 4 virtual datasets → 42 charts → 5 dashboards, all via
   `/api/v1/**` with Cookie+CSRF handling. Chart form_data uses
   `granularity_sqla`,`time_grain_sqla` and `time_range: "No filter"` so the
   dashboard time filter applies per chart. Pie charts require the singular
   `metric` field (the plugin crashes on `metrics`-only with
   *"Cannot read properties of undefined (reading 'label')"*). Dashboards are
   v2 layout with `native_filter_configuration` keyed to `datasetUuid`.
5. **Automated verification.** `dashboards/superset/verify.py` asserts object
   counts, unique names, layout integrity, filter config and live execution of
   the four dataset SQLs; `docs/bi/screenshots/` holds rendered evidence from a
   headless Edge session (all 42 chart slots render, zero JS errors).

## Consequences

- One extra container (~–1 container vs. Metabase-and-queue variants), SQLite
  metadata is fine for a single user and keeps the resource footprint low.
- Superset writes nothing to the warehouse: the connecting role cannot modify
  data by construction. The driver lands in `/app/.venv` site-packages via the
  system pip `--target` install + copy (both toolchains are CPython 3.10 on the
  official image), so the stock entrypoint runs unchanged.
- Trade-off accepted: `/api/v1/chart/{id}/data/` does not replay cached query
  results for API-created charts (that endpoint serves UI sessions), so
  verification relies on headless rendering + sql-lab execution instead.
- Future work: CI job to re-seed + verify, optional Presto/Druid for scale,
  and SSO — none needed for the local, single-admin deliverable.