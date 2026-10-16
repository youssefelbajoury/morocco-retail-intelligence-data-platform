# ADR-015 — Prometheus + Grafana over the existing audit model

- **Status:** Accepted
- **Date:** 2026-10-10
- **Phase:** 19 — Prometheus + Grafana observability
- **Deciders:** Project author (single-developer portfolio project)
- **Related:** [ADR-013 observability](ADR-013-observability.md)

---

## Context

The platform already runs the full pipeline (A → Bronze → Silver → dbt →
warehouse → BI) and records a structured audit trail in `ops.*` plus
`reports/observability/events.jsonl` (ADR-013). What is missing is a
**live, queryable view** of pipeline health, freshness, reconciliation and
Airflow task outcomes that survives without re-reading JSONL logs.

The phase requirements were explicit: stay under 8 GiB RAM, keep monitoring
**optional** (never change how the core stack is started), pin exact image tags
(never `latest`), expose only **bounded-cardinality** metrics, reuse the
existing `ops` views and audit events (no competing source of truth), avoid an
unnecessary always-running web application, and make it impossible for a failed
query to produce misleading metrics.

## Decision

1. **Optional Docker Compose profile.** Prometheus, Grafana and a project-owned
   exporter live in `docker-compose.observability.yml` behind the
   `observability` profile. The base/Airflow/Superset stacks are untouched and
   start exactly as before when the profile is absent.
2. **Pinned images.** `prom/prometheus:v3.13.4` (an LTS tag), the Grafana image
   `grafana/grafana:13.2.3` (the current `grafana` repo, not `grafana-oss`), and
   `python:3.12.13-slim-bookworm` for the exporter — all verified to exist on
   the registries before pinning.
3. **A hand-rolled exporter with zero new Python dependencies.** `prometheus-client`
   and `postgres_exporter` were both evaluated and rejected: the exposition
   format is small and stable, and modelling it in `src/mri/metrics/` keeps the
   image lean (only `psycopg`, already the project driver) and the payload fully
   deterministic and testable. No third-party web framework is used either —
   the stdlib HTTP server suffices for a batch platform.
4. **Existing sources only.** `sources.py` reads `ops.v_pipeline_metrics`,
   `ops.v_run_facts`, `ops.v_freshness`, `ops.pipeline_event`, PostgreSQL
   `pg_stat_*` and the Airflow metadata tables. The exporter writes nothing; the
   audit model remains the single source of truth.
5. **Bounded labels, enforced in code.** Every family exposes only
   `pipeline`/`stage`/`source`/`table`/`side`/`state`/`dag_id`/`datname`. High
   cardinality identifiers (runs, batches, files, customer/product ids) stay in
   the audit log.
6. **Fail-safe semantics.** An upstream failure drops the affected domain
   families and flips `mri_exporter_last_scrape_success` to `0` while
   incrementing `mri_exporter_scrape_errors_total`. A partial or stale value can
   never look fresh.
7. **Version-controlled operational assets.** Prometheus config + rules,
   Grafana datasource/dashboard provisioning and the four dashboards are all in
   `monitoring/` and validated in CI by `promtool` and by unit tests that forbid
   any panel referencing a metric the exporter does not emit.

## Consequences

- **Positive.** `make obs-up` gives a full live monitoring stack in ~0.5 GiB
  with bounded retention (15d/1 GiB). Dashboards and alerts cannot drift from
  reality: the "no fabricated metrics" guarantee is a test, not a convention.
  Removing monitoring is `make obs-down`; the platform's behaviour is
  unchanged without it.
- **Cost.** The exporter is maintained in-repo (~500 lines + tests). Airflow
  metrics need the Airflow metadata credentials; without them the Airflow
  dashboard is simply empty.
- **Honest gap.** Metrics are aggregated at stage granularity to keep
  cardinality bounded; per-run tracing remains in the observability JSONL.
- **Future path.** A real alertmanager route, or scraping other targets
  (MinIO, Superset), only requires adding a scrape job and a dashboard —
  no exporter change.