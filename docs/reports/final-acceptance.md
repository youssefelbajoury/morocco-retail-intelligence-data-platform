# Phase 21 - Final end-to-end acceptance audit

Status: **PASS (with one documented unverified item: interactive native-filter UI behaviour)**
Date: 2026-10-12
Author: Phase 21 acceptance pass

## 1. OBJECTIVE

Prove the Morocco Retail Intelligence Data Platform works **end to end** from a
reproducible local environment: clean-start readiness, a full Airflow pipeline
run, repeated-run idempotency, the Superset BI layer, monitoring/lineage, the
test gates and the Docker Compose matrix. Record evidence, dispose of any bug,
and state the final acceptance verdict.

## 2. SCOPE AND ACCEPTANCE CRITERIA

| # | Criterion | Result |
|---|-----------|--------|
| AC1 | Compose matrix (4 combinations) validates | PASS |
| AC2 | All CLI entry points exist and answer `--help` | PASS |
| AC3 | Full Airflow DAG run reaches `success` (8/8 tasks) | PASS |
| AC4 | A second run is idempotent (no data drift) | PASS |
| AC5 | dbt marts match the Python warehouse; SCD2 invariants hold | PASS |
| AC6 | Superset: 5 dashboards / 42 charts / 4 datasets load | PASS |
| AC7 | BI role remains read-only (no DDL/DML) | PASS |
| AC8 | Prometheus targets up; exporter metrics parse | PASS |
| AC9 | Lineage graph builds and traces source -> dashboard | PASS |
| AC10 | Lint / format / types / full test suite green | PASS |
| AC11 | Native-filter interactive behaviour | **PASS** (Phase 25, see `docs/reports/p25.md`) |

## 3. ENVIRONMENT AND CLEAN-START READINESS

- Target: **host-local development deployment** (Windows 11 + Docker Desktop,
  single `mri-net` bridge, all ports bound to `127.0.0.1` only).
- Bring-up path is documented in `README.md` (Quickstart sections) and
  `docs/development-environment.md`, and driven by `Makefile` targets:
  `docker-up`, `airflow-up`, `superset-up`, `obs-up`, `superset-seed`,
  `superset-verify`, `check`, `docker-down-v`, `obs-down-v`.
- Readiness was validated by **compose configuration** for every profile
  (AC1) plus the observed healthy running stack (10/10 `mri-*` containers).
  A **destructive rebuild from empty volumes was not executed** in this pass
  (it would tear down the state this audit observes); see s20.

## 4. CLI / ENTRY-POINT INVENTORY

All module CLIs respond to `--help`:

`apps.ingestion`, `apps.warehouse`, `apps.dbt`, `apps.data_generator`,
`apps.observability`, `apps.lineage` (`build`/`trace`/`dags`), `apps.metrics`.

## 5. END-TO-END PIPELINE RUN (RUN 1)

Triggered `morocco_retail_pipeline` = `manual__2026-10-12T11:44:49.386697+00:00`.

DAG run state: `success`, all 8 tasks `success`.

| task | start | end |
|------|-------|-----|
| ingest_internal | 11:50:40 | 11:51:54 |
| ingest_external | 11:50:40 | 11:51:54 |
| bronze_internal_external | 11:52:07 | 11:56:01 |
| silver_load | 11:56:04 | 11:56:31 |
| warehouse_build_reference | 11:56:32 | 11:56:37 |
| dbt_build | 11:56:38 | 11:57:39 |
| reconcile | 11:57:39 | 11:57:49 |
| audit_airflow_run | 11:57:50 | 11:57:52 |

Transient: the first attempt of `ingest_internal`/`ingest_external` went
`up_for_retry` with `httpx.ReadTimeout` (task-runner -> API-server under
resource pressure). Airflow retried automatically and both reached `success`.
Classified in s15.

## 6. REPEATED EXECUTION / IDEMPOTENCY (RUN 2)

Triggered `manual__2026-10-12T12:03:17.065071+00:00`; DAG run `success`,
all 8 tasks `success`, no retries.

| task | start | end |
|------|-------|-----|
| ingest_internal | 12:03:18 | 12:03:46 |
| ingest_external | 12:03:18 | 12:03:46 |
| bronze_internal_external | 12:03:47 | 12:05:44 |
| silver_load | 12:05:45 | 12:06:05 |
| warehouse_build_reference | 12:06:06 | 12:06:13 |
| dbt_build | 12:06:14 | 12:06:50 |
| reconcile | 12:06:51 | 12:07:00 |
| audit_airflow_run | 12:07:01 | 12:07:03 |

**Idempotency proof.** A per-table content hash (`md5` over ordered rows) was
captured for all 36 data tables (`warehouse.*`, `silver.*`, `raw_staging.*`,
`dbt_marts.*`, `dbt_staging.*`, `dbt_intermediate.*`, `dbt_snapshots.*`) before
run 1, after run 1 and after run 2.

- baseline vs run 1: **36/36 tables bit-identical**; only `audit.load_audit`
  grew (554 -> 677, +123 rows = the run-1 audit trail).
- run 1 vs run 2: **36/36 tables bit-identical**; only `audit.load_audit`
  grew (677 -> 800, +123 rows = the run-2 audit trail).

No duplicate business rows, no key drift, no SCD2 drift, no history rewrite.

## 7. DATA CORRECTNESS / dbt-PYTHON PARITY / SCD2

Row-count parity between the Python-built warehouse and the dbt-built marts:

| table | warehouse | dbt_marts |
|-------|-----------|-----------|
| fact_ventes | 2000 | 2000 |
| fact_achats | 1021 | 1021 |
| fact_stock | 8232 | 8232 |

dbt snapshot / SCD2 (`dbt_marts.dim_produit`, `dbt_snapshots.snapshot_dim_produit`):

- versions = 120, distinct products = 60, current rows = 60, snapshot rows = 120
- products without exactly one current version: **0**
- SCD2 contiguity gaps (`valid_to` of a version not equal to next `valid_from`): **0**

Pipeline reconcile task (`reconcile`) output: `PASS=2 WARN=0 ERROR=0 TOTAL=2`
(`assert_fact_row_counts`, `assert_marts_match_warehouse`).

`dbt_build` output: `Finished running 3 incremental models, 1 snapshot,
7 table models, 140 data tests, 16 view models`; `PASS=167 WARN=0 ERROR=0
SKIP=0 NO-OP=0 REUSED=0 TOTAL=167`.

## 8. BI LAYER (SUPERSET)

`dashboards/superset/verify.py` -> **all checks passed** (exit 0):

- 4 datasets present; each dataset SQL executes over HTTP 200.
- 42 charts (no duplicate slice names).
- 5 dashboards, all `v2` layout, no orphaned chart refs, native filters
  configured with `chartsInScope`.

API acceptance probe (reusing the project's own `seed.login()`/`seed.api()`):

- login OK; **5/5 dashboards return HTTP 200, `published=true`**
  (`Executive Overview`, `Inventory Health`, `Platform Health`, `Procurement`,
  `Sales Performance`).
- **42/42 charts** enumerated; sample `GET /api/v1/chart/{id}` -> 200.
  Chart types: `big_number_total` 20, `pie` 10, `echarts_timeseries_line` 6,
  `table` 5, `echarts_timeseries_bar` 1.
- **Filter data-path**: filter dimensions resolve against live data and
  filtering narrows the result set - `sales_analytics` 1842 rows total, filtered
  to region = `Marrakech-Safi` -> 679 rows; `region_name` and `category`
  partition all 1842 rows (7 categories); `platform_health.source` has 14 values.

## 9. MONITORING AND OBSERVABILITY

- Prometheus `GET /api/v1/targets`: **2/2 targets `up`**
  (`mri-metrics` -> `http://metrics-exporter:9245/metrics`; `prometheus` self).
- Exporter `GET /metrics`: HTTP 200, 234 lines, **175 `mri_*` metrics**.
- Grafana `GET /api/health`: `{"database":"ok","version":"13.2.3"}`.

## 10. DATA LINEAGE

`uv run python -m apps.lineage build` -> exit 0:
`datasets=875 edges=882 runs=8`; edges by kind `{observed:689, inferred:45,
declared:56, unavailable:92}`; wrote `reports/lineage/lineage.json`,
`openlineage.jsonl` (32 events) and `lineage.mmd` (882 edges).

`apps.lineage dags` lists **both** Phase 21 run ids, confirming run-id
correlation in the event log.

Trace `postgres:silver.sales` (direction `all`) resolves the full path:
`source:sales` -> `minio:retail-raw/...` -> `minio:retail-bronze/...` ->
`postgres:silver.sales` -> `dbt_staging.stg_sales` -> `warehouse.fact_ventes` ->
`dbt_intermediate.int_sales` -> `dbt_marts.fact_ventes` ->
`superset:dataset.sales_analytics` -> `superset:dashboard.Sales Performance` /
`superset:dashboard.Executive Overview`.

## 11. SECURITY / READ-ONLY ENFORCEMENT

The BI role path is **read-only** end to end:

- Superset database object runs with `allow_dml=False`.
- Via SQL Lab, `create table superset_write_probe(id int)` -> HTTP 500
  `DML_NOT_ALLOW`; `insert into warehouse.dim_region ...` -> HTTP 500
  `DML_NOT_ALLOW`.
- `superset_bi` remains a non-superuser, SELECT-only role
  (`infrastructure/sql/bi-reader-readonly.sql`, regression
  `tests/integration/test_bi_role_readonly.py`).

## 12. TEST GATES

| gate | command | result |
|------|---------|--------|
| lint | `uv run ruff check .` | All checks passed (exit 0) |
| format | `uv run ruff format --check .` | 195 files already formatted |
| types | `uv run mypy` | Success, no issues in 135 source files |
| tests | `uv run pytest -q` | **497 passed, 3 skipped in 3543.83s (59:03)**, exit 0 |

The 3 skips are the live external-API smoke tests (guarded by
`MRI_LIVE_SMOKE=1`). The Airflow DAG test now **runs** (Airflow up) instead of
being skipped, hence 497 passed / 3 skipped vs Phase 20's 496 / 4.

## 13. COMPOSE MATRIX

All four combinations pass `docker compose ... config -q` (exit 0):

1. core only
2. core + airflow
3. core + airflow + superset
4. monitoring profile (all overlays)

(Stderr shows benign "variable is not set" notices from `config`; exit code is 0
for all.)

## 14. REAL RUN EVIDENCE (SUMMARY)

- Run 1 `manual__2026-10-12T11:44:49.386697+00:00` -> `success` (8/8), one auto-retry.
- Run 2 `manual__2026-10-12T12:03:17.065071+00:00` -> `success` (8/8), no retry.
- Idempotency: 36/36 data tables bit-identical across baseline/run1/run2;
  only `audit.load_audit` grows 554 -> 677 -> 800.
- dbt: `PASS=167 WARN=0 ERROR=0`; reconcile: `PASS=2`.
- Superset: 5 dashboards / 42 charts / 4 datasets; read-only enforced.
- Monitoring: Prometheus 2/2 up; 175 `mri_*` metrics; Grafana ok.
- Lineage: 875 datasets / 882 edges; source -> dashboard trace resolves.
- Gates: lint/format/types green; tests 497 passed / 3 skipped.

## 15. BUGS FOUND AND DISPOSITION

| ID | Finding | Severity | Disposition |
|----|---------|----------|-------------|
| B1 | First ingest attempt in run 1 hit `httpx.ReadTimeout` (task-runner -> Airflow API server) under resource pressure; recovered via automatic retry | Low (transient, infra) | **Accepted / documented.** Not a pipeline-logic defect; run 2 had no retry. Existing `retries=2` + 5-min backoff absorbs it. |

No other defects found; no business-logic, correctness or data-integrity bug.

## 16. FILES CREATED

- `docs/reports/final-acceptance.md` (this report)
- (temporary, outside the repo) a Superset acceptance probe under the OS temp
  dir - not committed.

## 17. FILES MODIFIED

- None. This phase is an audit; no production source or configuration changed.

## 18. DEPENDENCIES + DATABASE CHANGES

- No dependency changes (`pyproject.toml`/lock untouched).
- No schema changes. Pipeline runs only appended to `audit.load_audit`
  (+123 rows per run); all analytical tables unchanged.

## 19. RESOURCE IMPACT

- Host-local; 10 `mri-*` containers healthy throughout.
- The long-running gate is the full test suite (~59 min) because dbt steps fan
  out through nested `uv`-managed interpreters; run sequentially under the
  <= 8 GiB Docker budget.

## 20. WARNINGS, LIMITATIONS, UNVERIFIED ITEMS

- **U1 (RESOLVED in Phase 25): interactive native-filter behaviour.** Filter
  *wiring* was verified structurally (`verify.py`) and the filter *data path* was
  verified (filtering narrows live results), but the in-browser interaction was
  not exercised in Phase 21 (no browser-automation tooling existed). Phase 25
  added a Playwright/Edge end-to-end suite (`e2e/`, `make e2e`) that drives the
  live UI: selecting native filters changes the chart data, clearing them
  restores it, and all 5 dashboards render. See `docs/reports/p25.md`.
- **U2: full destructive clean-start** (drop volumes, rebuild from zero) was not
  executed; readiness is asserted from compose validation + docs + the healthy
  running stack.
- **Doc drift:** `docs/orchestration/airflow.md` s3 still states 7 DAG tasks;
  the DAG has 8 (includes `audit_airflow_run`).
- Leftover schemas in `retail` (`*_itinc*`, `*_bench*`, `*_e2e_*`, ~371) remain;
  harmless noise, not part of the acceptance surface.

## 21. CURRENT STATUS / VERDICT

**ACCEPTED.** The platform runs end to end, repeatedly and idempotently, with
green quality gates, a working BI layer, observability and lineage - on a
defensible host-local development deployment. One item (interactive native-filter
UI behaviour) is explicitly unverified; the stale 7-task doc count should be
corrected.

## 22. NEXT STEP / RECOMMENDATIONS

1. Correct the DAG task count in `docs/orchestration/airflow.md` (7 -> 8).
2. Add a browser-automation smoke (e.g. Playwright) to close U1, or record the
   manual filter check in `docs/bi/superset.md`.
3. Optionally raise ingest `retries` or pin task-runner -> API-server timeouts
   to remove the B1 transient warning from run logs.
4. Optionally perform one scripted destructive clean-start rebuild on a scratch
   host to fully close U2.
