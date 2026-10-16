# ADR-014 — Data lineage as a derived, evidence-labelled view

- **Status:** Accepted
- **Date:** 2026-10-10
- **Phase:** 18 — Data lineage
- **Deciders:** Project author (single-developer portfolio project)
- **Related:** [ADR-013 observability](ADR-013-observability.md)

---

## Context

The platform spans seven layers (source files and external APIs → raw → Bronze →
Silver → dbt → warehouse → Superset). Each layer already emits audit evidence —
ingestion/Bronze JSON manifests, `audit.load_audit`, the unified
`ops.pipeline_event` stream and `reports/observability/events.jsonl`
(ADR-013) — plus source-controlled design artifacts (dbt `manifest.json`,
`silver/ddl.py:SOURCE_TABLES`, `warehouse/schema.py`,
`dashboards/superset/seed.py`).

What was missing was a **single end-to-end answer** to "where did this dataset
come from and what depends on it?". Building a bespoke third tracking system
would duplicate evidence that already exists and could drift from reality. The
phase also required **not upgrading the stack** to install a lineage
integration and **not making a new high-memory service mandatory** without
measurement.

## Decision

1. **Lineage is a derived read-only view.** `src/mri/lineage/` reads existing
   manifests, events and configuration; it writes nothing back to Postgres,
   MinIO or dbt. No new per-stage instrumentation is added to the pipelines.
2. **Every edge carries its evidence kind** — `observed`, `inferred`,
   `declared`, `unavailable` — and the kinds never masquerade as each other:
   design-time configuration is `declared`, an executed dbt model is `inferred`,
   and only a real event/manifest naming both endpoints is `observed`.
3. **Reuse existing identifiers verbatim** (`run_id`, `dag_run_id`, `batch_id`,
   SHA-256, file/object keys). Correlated runs are selected by `--dag-run-id`.
4. **Declared extractors read the mapping that already exists:** `SOURCE_TABLES`
   for Silver routing, dbt `parent_map` for the model DAG, the warehouse
   `render_load_statements` SQL for dim joins, and an `ast` parse of the Superset
   seed for BI objects.
5. **Observed extractors read the existing audit surface:** ingestion/Bronze
   manifests and observability events, with dbt `run_results.json` for executed
   nodes.
6. **Defer the OpenLineage backend.** `apps.lineage build` emits a minimal,
   spec-shaped OpenLineage `RunEvent` stream (`openlineage.jsonl`, schema
   `…/spec/2-0-2/OpenLineage.json#/definitions/RunEvent`) with stable UUIDv5 run
   ids and `parent` facets. `apache-airflow-providers-openlineage` is compatible
   (Airflow 3.3.2 ≥ 2.11) but requires a backend such as Marquez — a JVM
   service plus database — so no provider is installed and no backend is
   mandatory.
7. **Unavailable is first-class.** Inputs referenced by a run but never produced
   are flagged `unavailable` rather than silently dropped or fabricated.

## Consequences

- **Positive.** One command (`apps.lineage build`) yields the whole graph in
  JSON, OpenLineage JSONL and Mermaid; `trace` answers upstream/downstream for a
  single dataset; every claim is defensible because it names its evidence; the
  feature is purely additive (no pipeline code touched, zero new dependencies).
- **Cost.** The graph can be large at scale (782 edges at `tiny`), and the
  evidence is only as complete as the run that produced it.
- **Honest gaps.** A partial run legitimately yields `unavailable` inputs (e.g.
  raw objects present from earlier runs but not re-ingested); this is treated as
  signal, not error.
- **Future path.** A Marquez deployment can consume the emitted JSONL, or the
  emitter can POST to a collector, without re-instrumenting the pipeline.
