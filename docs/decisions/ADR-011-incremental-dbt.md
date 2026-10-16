# ADR-011 — Incremental dbt materialization for transactional facts

- **Status:** Accepted
- **Date:** 2026-10-09
- **Phase:** 14 — Incremental processing + scalability
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phase 08/09/10 made Silver constrained and the dbt analytics layer
authoritative, but every `dbt build` re-materialized the three mart facts
from scratch:

- `fact_ventes` — one row per sale line item (`grain = sale_id`)
- `fact_achats` — one row per purchase receipt line (`grain = purchase_id`)
- `fact_stock` — one row per store/product/date snapshot

Because these tables grow roughly linearly with the generated dataset, a
full refresh regenerates *all* rows every build (dims + snapshot + facts +
tests). At the benchmarked scales (100K → 5M sales) that cost becomes the
critical path of every load, for zero new information most of the time. The
phase had to settle five questions:

1. **Which marts can be incremental without breaking semantics?** A
   dimension or fact whose historical rows must be recomputed on every run
   is not a candidate; one where new/updated rows arrive with a detectable
   marker is.
2. **What is a correct, deterministic watermark?** Silver rows carry
   `event_date` (the business date), `ingestion_timestamp` (when Bronze
   promoted the raw file, second precision) and a `batch_id` derived from
   the raw source filename. Only one of these is monotonic in arrival order.
3. **How do late-arriving and corrected sales behave end-to-end?** The
   pipeline must define — and tests must prove — what happens to a sale that
   lands after its event date, a sale that is corrected, and a re-ingested
   duplicate file.
4. **Does the Silver batch load still hold at 1M+ rows?** Silver is the
   single upstream; making dbt incremental must not weaken the loading
   contract (batch uniqueness, `already_loaded` skip, reconciliation).
5. **How do we *prove* incremental == full-refresh?** dbt's own semantics
   must be verified at scale: row counts, total measures, and — for the
   SCD2 dimension — that current dimension values resolve identically.

## Decision

1. **All three transactional facts become incremental; all dimensions stay
   full-refresh.** `fact_ventes`, `fact_achats` and `fact_stock` are the
   only marts whose grain grows with the source event volume; their rows are
   immutable-as-arrived (Silver upserts on the natural key) so a
   `merge` on that key is a safe idempotent apply. Dimensions stay
   `table` unless a consumer later justifies change capture: region/canal/
   client/magasin/promotion are bounded and dimension/date rows do not
   retroactively change. `dim_produit` is inherently incremental already —
   its history is a dbt snapshot, which only appends changed versions.
2. **Watermark = per-row `ingestion_timestamp` from Bronze provenance,
   compared strictly-greater than the fact's own max.** Every Silver row
   inherits the provenance stamp Bronze wrote at promotion time
   (`bronze_schema.py` `add_provenance`; second precision via
   `now_iso()`). `stg_*` already exposed it; the intermediate models now
   pass it through; each fact selects `s.ingestion_timestamp` and filters
   `{% if is_incremental() %}` `where ingestion_timestamp > (select
   max(ingestion_timestamp) from {{ this }})` `{% endif %}`.
3. **Late-arriving data is handled by the watermark, not by event date.**
   A late sale gets a *fresh* `ingestion_timestamp` when its corrected file
   is re-promoted, so it sits permanently after the fact watermark and is
   picked up on the next incremental build — regardless of how old
   `event_date` is. Corrected lines arrive under new provenance and upsert
   onto their existing `sale_id`/`purchase_id`/`(snapshot_date, store_id,
   product_id)` rows. A duplicated batch re-ingest re-produces the exact
   same rows (`already_loaded` skip at Silver; identical content) so the
   merge is a no-op.
4. **dbt config: `materialized='incremental'`, `incremental_strategy='merge'`
   (valid in dbt-postgres 1.11+), `unique_key = sale_id` /
   `purchase_id` / `['snapshot_date','store_id','product_id']`.** A JSON
   `--vars` override (`silver_schema`, `warehouse_schema`, `schema_suffix`)
   keeps every run isolated exactly as before, so a full-refresh build in a
   throwaway suffix still produces byte-identical results to the
   incremental one.
5. **The Silver loader is unchanged: it remains the correctness gate.** No
   safety guarantee was removed or relaxed — upsert-on-conflict, batch
   `already_loaded` skip, and post-load reconciliation all stay. "Silver
   suitability at scale" is a *measured claim* (benchmarks) rather than a
   redesign.
6. **Correctness is asserted, not assumed, at every scale.** The dbt tests
   already guarantee `full-refresh == incremental` for the *first* build
   (`assert_fact_row_counts`, warehouse parity). The new incremental suite
   drives the *steady-state* case (late sale, corrected sale, duplicate
   batch) on a live PostgreSQL, and the benchmark harness checks row counts
   and aggregate totals after an incremental apply against a from-scratch
   facts full-refresh, plus fact-count == Silver-count reconciliation.

## Why not the alternatives

| Alternative | Why not chosen |
|-------------|----------------|
| Watermark on `event_date` | `event_date` is a business date, not an arrival marker. Late sales and corrections can carry dates *behind* the watermark, so an event-date filter would silently drop them forever; a date watermark is also coarse (days) and meaningless for re-processing. |
| Watermark on `batch_id` | `batch_id` derives from the raw filename (`sales__<run>`), is coarse (a whole file re-promoted changes nothing for rows inside it that were already applied) and conflates provenance with ordering; it cannot express a *delta inside* a batch. |
| `incremental_strategy='append'` | Append never updates; a corrected sale would leave a stale row and a new one — the SCD2/upsert guarantees Silver already provides would be thrown away. Merge re-runs Silver's own upsert semantics. |
| `incremental_strategy='delete+insert'` | Delete-by-timestamp then re-insert is more DML for the common no-op case and reorders rows; merge is the direct statement of "apply what changed". |
| Microbatch incremental for Postgres | dbt-postgres supports `microbatch`, but batch partitioning over `ingestion_timestamp` adds event/`begin`/`end` bookkeeping that yields nothing over a single strictly-greater filter on one watermark column. |
| Incremental *dimensions* too | Bounded dimensions gain nothing (12 regions, ~40 stores) and complicate the `grain = PK` invariant. `dim_produit` history is already handled by the snapshot, which is incremental by construction. |
| Read the watermark from a manifest table instead of `{{ this }}` | A separate state table is more schema to keep in sync; the facts' own data is the authoritative max (`max(ingestion_timestamp)` on the table being merged) and needs no new object. |
| Drop Silver / make dbt read Bronze directly | Phases 08/10 already rejected this (duplicated cleaning + lost audit registry); scaling Silver — not replacing it — is the task. |

## Consequences

- **Positive:** `dbt build` on an unchanged day reprocesses zero fact rows
  (no-op) instead of rebuilding millions; a late batch only applies its
  delta; correctness is provable by the incremental test suite and the
  benchmark's full==incremental + reconciliation checks; the change is
  invisible to consumers (facts keep the same columns; `ingestion_timestamp`
  is additive); CI's parity test still passes because `assert_marts_match_warehouse`
  compares explicit columns and no-ops when `warehouse_schema` is absent.
- **Negative:** the watermark is **second-precision** (`now_iso`), so two
  batches promoted within the same second share a watermark — the loader's
  `already_loaded`/upsert still dedupes, but an integration test between two
  runs must sleep >1s (documented in the suite), and a corrected row
  promoted within the same second as its original would be caught by the
  merge upsert on the key (not by the watermark); the facts gain an
  `ingestion_timestamp` column (additive, but consumers must not join on
  it as a business date); incremental tables are stateful — the documented
  manual `--full-refresh` remains the reset path.
- **Follow-ups:** wire the no-op/delta phases into Airflow's dbt task so
  daily loads report how many rows each run actually applied; consider a
  plan cache or `--empty` no-op detection for "nothing new upstream" runs;
  re-run the 1M/5M benchmarks after any schema or macro change that touches
  the fact paths.

---

## Verification record

- `python -m apps.dbt parse`/`compile` — all 26 models, snapshot and 140
  data tests compile.
- `tests/integration/test_incremental_postgres.py` — live-PostgreSQL suite
  proving fresh incremental == full-refresh, late sale insert, corrected
  sale update-in-place, duplicate-batch no-op + idempotent re-run, late
  purchase + new stock snapshot (see Phase 14 report §10).
- `benchmarks/dbt_benchmark.py` — 100K/1M/5M harness measuring full vs no-op
  vs delta runtime, rows/sec, DB size and peak RSS, with
  `equal_full_and_incremental` and `reconciliation_balanced` flags
  (see Phase 14 report §7–§8).