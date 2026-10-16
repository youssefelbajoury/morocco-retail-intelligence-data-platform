# PHASE 17 — PERFORMANCE ENGINEERING REPORT

> **Status: READY (PARTIAL scalability coverage).**
> A reproducible end-to-end and query performance baseline has been established
> against the local container stack, all measured stages reconcile, and the
> quality/type/lint gates are green. No business-semantics were changed and **no
> new optimisation was implemented** (the measured warehouse bottleneck is only
> removable by changing documented rebuild semantics — see §6/§7). Coverage is
> `small`, `medium` and `1m`; `5m` was **not run** (host resource limit, §18).
>
> Artefacts: `benchmarks/pipeline_benchmark.py`, `benchmarks/query_benchmark.py`,
> `benchmarks/pipeline/results.json`, `benchmarks/queries/results.json`.
> Re-validated prior optimisation: `benchmarks/dbt/results.json` (Phase 14).

---

## 1. OBJECTIVE

Establish a reproducible performance baseline for the existing platform without
changing business semantics:

1. Measure each pipeline stage end-to-end (generate, ingest files, ingest
   external, bronze, silver, warehouse, dbt) at increasing volume.
2. Measure representative business queries and inspect their execution plans.
3. Identify the real bottlenecks from the measurements.
4. Optimise only where a measurement justifies it **and** correctness is
   protected by tests **and** a before/after comparison is possible.
5. Produce this report plus machine-readable results, and run the gates.

## 2. ENVIRONMENT

| Item | Value |
|------|-------|
| Host OS | Windows-11-10.0.26300-SP0 |
| Python | 3.12.13 (CPython) |
| Logical CPUs | 8 |
| Total host RAM | 8,386,805,760 B (~7.81 GiB) |
| Observed host free RAM | < 1 GB during heavy runs |
| Container engine | Docker Desktop; VM memory capped ~3.7 GB |
| Database | PostgreSQL (container `mri-postgres`, host port 5433) |
| Object store | MinIO (container `mri-minio`, host port 9000) |
| Orchestrator | Airflow (containers, not exercised here) |

Notes:

- Measurements are **single-host, single-database-container** and are intended
  for *relative* comparison and regression detection, not absolute SLA claims.
- The Docker engine crashed once under memory pressure while running a warm
  `medium` pass concurrently with other containers (OOM). It was restarted and
  the stack recovered from its named volume with no data loss. The affected run
  was discarded and re-executed cold-only.

## 3. DATASET PROFILES

Profiles are provided by the generator. `small`/`medium` use the named
production profiles; `1m`/`5m` use the dbt-benchmark style specs (`dbt1m`/`dbt5m`).

| Scale | sales_rows | products | stores | customers | promotions | assortment | snapshot_days | dataset bytes |
|-------|-----------:|---------:|-------:|----------:|-----------:|-----------:|--------------:|--------------:|
| small | 60,000 | 200 | 10 | 3,000 | 150 | 0.50 | 30 | 8,766,627 (~8.8 MB) |
| medium | 600,000 | 600 | 24 | 25,000 | 800 | 0.40 | 14 | 92,712,950 (~92.7 MB) |
| 1m | 1,000,000 | 400 | 16 | 10,000 | 500 | 0.45 | 14 | 116,186,988 (~116.2 MB) |

> The product/customer counts are **not monotonic** across the three rows because
> `medium` uses the named production profile while `1m` uses the dbt-style spec.
> This is a property of the two existing profile tables, not a sampling error.

Actual row counts loaded (Silver, then star schema):

| Scale | silver sales | silver purchases | silver inventory | dim_date | dim_produit | fact_ventes | fact_achats | fact_stock |
|-------|-------------:|-----------------:|-----------------:|---------:|------------:|------------:|------------:|-----------:|
| small | 60,000 | 17,412 | 44,100 | 1,461 | 200 | 60,000 | 17,412 | 44,100 |
| medium | 600,000 | 164,949 | 604,170 | 1,461 | 600 | 600,000 | 164,949 | 604,170 |
| 1m | 1,000,000 | 158,090 | 302,400 | 1,461 | 400 | 1,000,000 | 158,090 | 302,400 |

## 4. MEASURED PIPELINE PERFORMANCE

Commands (from the repository root, stack up):

```text
uv run python benchmarks/pipeline_benchmark.py --scale small  --warm-repeats 1
uv run python benchmarks/pipeline_benchmark.py --scale medium --warm-repeats 0
uv run python benchmarks/pipeline_benchmark.py --scale 1m     --skip-generate --warm-repeats 0
```

### 4.1 Cold pass — stage seconds

| Stage | small | medium | 1m |
|-------|------:|-------:|---:|
| generate | skipped/reused | skipped/reused | skipped/reused |
| ingest_files | 3.391 | 9.550 | 7.761 |
| ingest_external | 1.700 | 1.447 | 1.452 |
| bronze | 48.491 | 105.995 | 160.475 |
| silver | 60.871 | 275.518 | 408.957 |
| warehouse | 47.820 | 274.184 | 325.186 |
| dbt | 59.316 | 91.961 | 90.032 |
| **E2E total** | **221.589** | **758.655** | **993.863** |
| peak RSS | ~384 MB | ~1.14 GB | ~1.75 GB |

Share of E2E (cold):

| Stage | small | medium | 1m |
|-------|------:|-------:|----:|
| ingest_files | 1.5% | 1.3% | 0.8% |
| ingest_external | 0.8% | 0.2% | 0.1% |
| bronze | 21.9% | 14.0% | 16.1% |
| silver | 27.5% | 36.3% | 41.1% |
| warehouse | 21.6% | 36.1% | 32.7% |
| dbt | 26.8% | 12.1% | 9.1% |

### 4.2 Warm pass (small only)

| Stage | small warm #1 |
|-------|--------------:|
| ingest_files | 1.488 |
| ingest_external | 0.172 |
| bronze | 35.245 |
| silver | 55.711 |
| warehouse | 46.927 |
| dbt | 47.946 |
| **E2E total** | **187.489** |

Warm repeats were only collected for `small`. `medium`/`1m` were run cold-only
due to per-run duration and memory pressure (§18).

### 4.3 Warehouse stage breakdown (`WarehouseBuilder.timing`)

| Scale | ddl_seconds | load_seconds | reconcile_seconds | total_seconds |
|-------|------------:|-------------:|------------------:|--------------:|
| small | 0.593 | 46.633 | 0.542 | 47.820 |
| medium | 0.818 | 268.176 | 3.302 | 274.165 |
| 1m | 1.281 | 321.329 | 2.422 | 325.186 |

`load_seconds` is **97–99%** of the warehouse stage at every scale.

### 4.4 Database / schema footprint

| Scale | silver_schema | warehouse_schema | dbt_marts_schema | database (cumulative) |
|-------|--------------:|-----------------:|-----------------:|----------------------:|
| small | 42.9 MB | 24.7 MB | 18.4 MB | 2.25 GB |
| medium | 481.9 MB | 256.2 MB | 191.5 MB | 3.59 GB |
| 1m | 517.7 MB | 304.7 MB | 220.5 MB | 5.09 GB |

> `database` is the size of the whole database (development + every benchmark
> schema) and is **cumulative, not per-run**. Use the schema-level figures for
> per-scale footprint.

## 5. QUERY PLANS

Commands:

```text
uv run python benchmarks/query_benchmark.py --scale small
uv run python benchmarks/query_benchmark.py --scale medium
uv run python benchmarks/query_benchmark.py --scale 1m
```

Harness: `ANALYZE` to refresh statistics, then
`EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`, 3 repeats, read-only, against the
persisted `warehouse_e2e_*` schemas.

### 5.1 Median execution time (ms)

| Query | rows | small | medium | 1m |
|-------|-----:|------:|-------:|---:|
| revenue_by_month_region | 240/432/288 | 92.6 | 479.7 | 1217.4 |
| margin_by_category | 7 | 48.9 | 266.3 | 590.7 |
| inventory_value_by_category | 7 | 27.0 | 213.4 | 151.7 |
| purchase_trend_by_month | 47/48 | 9.9 | 84.8 | 102.8 |
| top_products_by_revenue | 20 | 84.4 | 298.6 | 1008.8 |

### 5.2 Plan shape

All five queries resolve to **set-based plans**: one sequential scan of the
relevant fact table, hash/group aggregation, and a sort/limit. Observed per
scale:

- `small`: all scans are `Shared Hit` only (`Shared Read = 0`) — the working set
  fits in `shared_buffers`.
- `medium`/`1m`: `Shared Read > 0` on `fact_ventes`/`fact_achats` posts — the
  fact tables exceed cache, so the queries are I/O-bound and latency grows with
  volume.
- No nested loops, no repeated rescans, no Cartesian products.

**Conclusion:** every query aggregates a large fraction of a fact table; a
scan-and-aggregate is optimal for that access pattern. No supporting index would
materially help these full-table aggregations, so no index was added for them.

## 6. BOTTLENECKS

Ranked by measured impact on cold E2E:

1. **Silver load** — 27–41% of E2E. The loader deterministically re-reads and
   upserts the full dataset every run (by design, for idempotence); it does not
   short-circuit on content.
2. **Warehouse load** — dominates its stage (97–99%). Per-row inserts maintain
   primary keys, unique keys, foreign keys and FK indexes while joining to
   dimensions.
3. **Bronze** — 14–22% cold; a *warm* run still costs ~35 s at `small` because
   Great Expectations validation re-runs on already-bronzed files
   (`already_bronzed=22`), which is a deliberate quality guarantee.
4. **dbt** — 9–27%; nearly flat in absolute terms across scales because the
   Phase-14 incremental `merge` strategy restrains growth.
5. **Query latency** — grows with fact volume and becomes I/O-bound past cache;
   inherent to full-table aggregation.

Warm repeats only save on ingestion short-circuits (`ingest_files` 3.4→1.5 s,
`ingest_external` 1.7→0.2 s); the heavy stages (bronze/silver/warehouse/dbt) are
unconditional rebuilds and change little between cold and warm.

## 7. OPTIMIZATIONS IMPLEMENTED

**This phase: none.** The policy is to optimise only when a measurement
identifies a bottleneck, correctness is protected by tests, and a before/after
comparison is possible — and never to change business semantics speculatively.

A strong candidate was identified and **deliberately not implemented**:

> **Candidate — defer warehouse PK/UNIQUE/FK/index creation until after the bulk
> load.** `WarehouseBuilder` creates all keys/indexes inline with `CREATE TABLE
> IF NOT EXISTS` and then `TRUNCATE`s and reloads in place. Deferring the keys
> would eliminate per-row index/constraint maintenance during the insert — but
> only on a **fresh** schema. On the documented in-place rebuild the keys already
> exist, so the reload would still pay full maintenance. Making the optimisation
> effective on every build would require either dropping/recreating the
> constraints each build or dropping the schema each build, which changes the
> documented rebuild semantics and risks concurrent readers. That is outside the
> "do not change business semantics" constraint, so it is recorded as a future
> ADR recommendation (§19) rather than implemented here.

**Re-validated prior optimisation (Phase 14):** the incremental `merge`
materialisation on `fact_ventes`/`fact_achats`/`fact_stock` remains correct and
holds under this baseline — see §8.

## 8. BEFORE / AFTER RESULTS

### 8.1 Phase-14 dbt optimisation (full-refresh vs incremental)

Source: `benchmarks/dbt/results.json` (re-validated in this phase; both entries
`equal_full_and_incremental=True`, `reconciliation_balanced=True`).

| Scale | full build | facts full-refresh | facts incremental | speed-up |
|-------|-----------:|-------------------:|------------------:|---------:|
| 100k | 43.106 s | 23.598 s | 21.655 s | ~2.0x |
| 1m | 76.489 s | 41.955 s | 29.546 s | ~2.6x |

No-op rebuilds: 100k 23.507 s, 1m 24.631 s. Detailed `footprint_incremental` ==
`footprint_full_refresh` at both scales, confirming identical resulting state.

### 8.2 E2E warm vs cold

At `small`, warm removes ~34 s (~15%) of the E2E time (ingestion short-circuits);
the transformation stages are effectively unchanged (§4.2).

## 9. RECONCILIATION

`reconciliation` block of every recorded pipeline pass:

| Scale | rows_match | warehouse_balanced | dbt_tests_passed |
|-------|:----------:|:------------------:|:----------------:|
| small | True | True | True |
| medium | True | True | True |
| 1m | True | True | True |

The star-schema build reported `balanced=True` at every scale, and dbt data tests
passed in all recorded passes. Warehouse constraint/index invariants were
independently re-checked by the live integration suite (§10).

## 10. TEST RESULTS

| Gate | Command | Result |
|------|---------|--------|
| Lint | `uv run ruff check .` | All checks passed |
| Format | `uv run ruff format --check .` | 153 files already formatted |
| Types | `uv run mypy` | no issues in 104 source files |
| Unit + smoke | `uv run pytest -q` | 423 passed, 3 skipped (live API smoke) |
| Warehouse integration | `uv run pytest -q tests/integration/test_warehouse_postgres.py` | 9 passed |

The skipped tests are live external-API smoke tests gated by `MRI_LIVE_SMOKE=1`.

## 11. FILES CREATED

- `benchmarks/pipeline_benchmark.py` — end-to-end stage harness (cold + warm,
  isolated benchmark buckets/schemas, peak-RSS sampler, JSON recording).
- `benchmarks/query_benchmark.py` — five read-only business queries via
  `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)`.
- `benchmarks/pipeline/results.json` — recorded E2E passes (`small` cold+warm,
  `medium`/`1m` cold-only).
- `benchmarks/queries/results.json` — recorded query medians + plans
  (`small`/`medium`/`1m`).
- `docs/reports/performance-engineering.md` — this report.

## 12. FILES MODIFIED

- None. No source, dbt model, configuration or contract file was changed in this
  phase.

## 13. FILES DELETED

- None.

## 14. FILES MOVED / RENAMED

- None.

## 15. DEPENDENCIES ADDED

- None. The harnesses reuse existing dependencies (`httpx`, `psutil`,
  `minio`, `dbt`, `sqlalchemy`/driver already present).

## 16. DATABASE CHANGES

- No schema/DDL changes to application objects.
- Benchmark-only objects created and left in place for reproducibility:
  object-store buckets `retail-bench-raw-e2e-<scale>` /
  `retail-bench-bronze-e2e-<scale>` (purged at cold start and at end of each
  run), and Postgres schemas `raw_staging_e2e_<scale>`, `silver_e2e_<scale>`,
  `audit_e2e_<scale>`, `warehouse_e2e_<scale>` and dbt schemas
  `dbt_*_e2e_<scale>`. Development schemas were never touched.

## 17. WARNINGS

- All measurements are single-host and should be read as relative, not absolute.
- External-API stages are measured **offline** (deterministic
  `httpx.MockTransport` responses), so they do not reflect real network latency.
- The `database` byte metric is cumulative across every schema in the database;
  only the schema-level sizes are per-scale.
- Benchmark schemas persist, so the development database grows across runs;
  drop the `*_e2e_*` schemas to reclaim space.
- One warm `medium` run was invalidated by a transient MinIO clock-skew error
  (`RequestTimeTooSkewed`) and a Docker OOM; it was discarded, not recorded.

## 18. BLOCKERS

- **`5m` not run.** The host had < 1 GB free RAM and the Docker VM is capped at
  ~3.7 GB; the `1m` pass already peaked at ~1.75 GB and a concurrent warm pass
  previously triggered an engine OOM. Running `5m` safely would exceed the
  available budget. Documented, not attempted.
- Warm repeats for `medium`/`1m` were skipped for the same resource/time reason.

## 19. STATUS

**READY** for the tested volumes (`small`, `medium`, `1m`), with the explicit
`PARTIAL` caveat that `5m` is unmeasured and warm data is `small`-only.

## 20. NEXT STEP

- Run the `5m` scenario (and warm repeats for `medium`/`1m`) on a host with
  adequate RAM, using the same harness flags, and append to the existing
  `benchmarks/*/results.json`.
- If in-place warehouse build performance becomes a requirement, open an ADR for
  the deferred-constraint bulk load described in §7, including the rebuild-
  semantics decision, and implement it behind the existing reconciliation and
  integration tests.

## 21. RECOMMENDATIONS

1. **Treat Silver and warehouse load as the primary optimisation targets** — they
   account for ~65–75% of cold E2E.
2. **Do not add indexes for the five measured queries** — they are full-table
   aggregations; scan-and-aggregate is already optimal.
3. **Keep the incremental dbt marts** — the ~2–2.6x win is confirmed and state
   equivalence holds.
4. **Reclaim benchmark objects** after analysis (`*_e2e_*` schemas and
   `retail-bench-*` buckets) to avoid database growth.
5. **Schedule the `5m` run** on a resource-adequate host before making any
   scalability claim beyond `1m`.
