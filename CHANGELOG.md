# Changelog

All notable changes to the Morocco Retail Intelligence Data Platform are
recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/)
and the project uses phase-based milestones.

## [Unreleased]

### Added

- **Phase 28 — Final security & release-readiness remediation.** Rotated the
  Airflow Fernet key (official two-key transition + `rotate-fernet-key`), the
  Airflow JWT secret, the Airflow admin password, the exposed Airflow
  metadata-DB password, the Superset admin password and the `superset_bi`
  warehouse credential (Superset connection re-pointed), each verified
  end to end. Also: reconciled the dashboard figures, inventoried the residual
  test schemas (none dropped), moved the secret scanner to
  `scripts/scan_secrets.py`, made the CI trigger match the `master` branch, and
  brought README/CHANGELOG/ARCHITECTURE status up to Phase 28. Report:
  `docs/reports/p28.md`.

- **Phase 27 — Git & release-readiness audit.** Git + secret inventory (29
  heuristic hits, all benign), license-decision and release-readiness reports,
  an 8-commit publication plan, and an `.env.example` mojibake fix. Reports:
  `docs/reports/p27.md`, `docs/reports/license-decision.md`,
  `docs/reports/release-readiness.md`.

- **Phase 26 — Clean-start reproduction.** Full pipeline, Airflow and Superset
  rebuilt from scratch on an isolated, non-destructive scratch stack (`mri-cs`),
  all green; the normal stack was restored. Report: `docs/reports/p26.md`.

- **Phase 25 — Superset native-filter fix + E2E suite.** `filter_select` now
  emits `datasetId`/`datasetUuid` (dashboard 1 repaired), with a regression
  assertion and an 8-test headless-browser suite under `e2e/`. Report:
  `docs/reports/p25.md`.

- **Phase 24 — Least-privilege PostgreSQL roles.** `retail_app` demoted to a
  non-superuser owner, `retail_admin` break-glass, `superset_bi` SELECT-only,
  plus `infrastructure/sql/least-privilege-roles.sql` and 7 security
  integration tests. Documented in `docs/security/security-hardening.md`.

- **Phase 23 — Credential rotation + stale-schema inventory.** Rotated the
  warehouse app password, MinIO root and Grafana admin credentials; inventoried
  162 stale schemas (≈12.7 M estimated rows) without dropping any. Documented in
  `docs/security/security-hardening.md`.

- **Phase 22 — Portfolio packaging.** Rewritten README (two-minute overview,
  accurate status, Windows + WSL2 + Docker quick start, measured results,
  screenshots). `CONTRIBUTING.md`, `CHANGELOG.md`, troubleshooting runbook,
  data-dictionary landing page, limitations & roadmap. Corrected stale counts
  (DAG 7→8 tasks, dbt tests 131→140, nodes 158→167) and status claims across
  `README.md`, `ARCHITECTURE.md`, `docs/architecture/observability.md`,
  `docs/data-model/dbt.md`, `docs/orchestration/airflow.md`, `ADR-008`.

- **Phase 21 — Final end-to-end acceptance audit.** Two green DAG runs, byte-level
  idempotency proof for 36 data tables, dbt/Python parity, Superset/monitoring/
  lineage verification. Report: `docs/reports/final-acceptance.md`.

- **Phase 20 — Security & hardening.** Secrets audit, least-privilege BI role,
  digest-pinned MinIO (CVE-2025-62506 resolved), regression tests, and
  `docs/security/security-hardening.md`.

- **Phase 19 — Prometheus + Grafana observability.** Optional `observability`
  compose profile, zero-dependency `mri-metrics` exporter, 4 provisioned Grafana
  dashboards, alert rules, promtool-validated config.

- **Phase 18 — Data lineage.** Evidence-labelled graph (observed/inferred/
  declared/unavailable) emitted as JSON, OpenLineage JSONL and Mermaid.

- **Phase 16 — Structured audit logging + pipeline metrics.** Canonical JSONL
  event log mirrored into the PostgreSQL `ops` schema.
- **Phase 15 — Contract drift detection.** Contracts and runtime implementation
  proven never to diverge.
- **Phase 14 — Incremental dbt materialization.** Watermark + `merge` strategy
  for the three fact models, with benchmarks proving full/incremental parity.
- **Phase 13 — GitHub Actions CI/CD.** 8-job pipeline (configured; not yet run
  remotely).
- **Phase 12 — Apache Superset BI layer.** Read-only role, 4 datasets, 42 charts,
  5 dashboards with native filters.
- **Phases 00–11 — Foundation through orchestration.** Environment, synthetic
  generator, containers, file + external ingestion, Bronze + GX gate, Silver,
  star-schema warehouse, dbt analytics layer (staging/intermediate/marts/SCD2),
  Apache Airflow orchestration.

### Notes

- All data is **synthetic**; external sources are public and keyless.
- No open-source license has been chosen yet (see README).
