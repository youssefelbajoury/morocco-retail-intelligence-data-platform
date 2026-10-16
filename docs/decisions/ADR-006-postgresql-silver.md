# ADR-006 — PostgreSQL Silver: contract-derived schemas, staging upsert, audit-as-registry

- **Status:** Accepted
- **Date:** 2026-10-07
- **Phase:** 08 — Silver load (promoted Bronze → PostgreSQL)
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phase 06/07 left the platform with typed, quality-gated Parquet in MinIO:
14 contract sources, promotion markers under `_promoted/`, provenance on every
part, and the guarantee that *only* promoted batches are consumable. Before
dbt (Phase 10) can model anything, those batches have to become **tables** in
the stack's single relational engine — PostgreSQL 16, already running as a
container since Phase 03 but with no schema at all.

Five questions had to be settled:

1. **Where do table definitions come from?** 14 YAML contracts already
   declare columns, types, nullability and primary keys — yet the SQL layer
   also needs foreign keys, merged-table rules and provenance columns that
   contracts do not (and should not) express.
2. **How does bulk data physically enter PostgreSQL?** Fact batches reach
   1.4M rows (medium scale) and must land through one mechanism that also
   survives duplicate keys — `regions` batches genuinely overlap.
3. **What makes a rerun a no-op?** The platform's idempotence story so far
   is content-derived object keys; PostgreSQL has no such notion, and table
   row counts cannot tell *which* batch produced which rows.
4. **What happens when one batch fails?** FK/CHECK violations are expected
   (hand-edited markers, partial local states); aborting the whole run would
   make large loads fragile, but silently continuing past a bad batch would
   break reconciliation.
5. **Does Silver re-validate data?** Great Expectations already gates every
   batch at Bronze (ADR-004); duplicating business rules in SQL would create
   two divergent definitions of "valid".

## Decision

1. **Three schemas, one direction: `raw_staging → silver → audit`.**
   `raw_staging` holds 13 constraint-free heaps (`TRUNCATE`-reused per
   batch); `silver` holds the 13 business tables with contract primary keys,
   10 foreign keys and structural checks; `audit` holds `load_audit`. A batch
   loads as `TRUNCATE + COPY + INSERT … SELECT … ON CONFLICT DO UPDATE +
   success audit row` inside **one transaction** — either the batch is fully
   present (and registered) or Silver is exactly as before, with a `failed`
   audit row explaining why.
2. **DDL is derived from the contracts at build time** (`silver.ddl`
   `build_table_specs`): business columns = contract columns union-merged
   per table, plus six provenance columns (`source`, `source_file`,
   `source_hash`, `batch_id`, `ingestion_run_id`, `ingestion_timestamp`).
   Types map through a fixed five-entry table (`string→TEXT`, `integer→
   BIGINT`, `number→DOUBLE PRECISION`, `boolean→BOOLEAN`, `date→DATE`).
   14 sources collapse to 13 tables because `open_meteo_history` and
   `open_meteo_forecast` describe the same entity (`weather`). Foreign keys
   are *not* in the contracts; they are declared once in `ddl.FOREIGN_KEYS`
   and emitted as self-healing `DO $$ … IF NOT EXISTS (pg_constraint)` blocks
   so the loader coexists with any pre-existing database state.
3. **`weather` gets no primary key.** Its two sources have conflicting,
   independently nullable date keys; a PK would force `NOT NULL` on both
   dates and a single composite key cannot represent "one observation OR one
   forecast per region/day". Instead: one **partial unique index per source**
   (`(region_id, observation_date) WHERE observation_date IS NOT NULL`,
   likewise forecasts) plus `ck_weather_dates` requiring at least one date.
   The same `IS NOT NULL` predicate is reused in that source's `ON CONFLICT`
   clause, so upserts, uniqueness and the check constraint agree.
4. **The audit table is also the load registry.** A partial unique index
   `ux_load_audit_success (batch_id, target_table) WHERE status='success'`
   makes "this batch is loaded into this table" a database fact; at run start
   the loader reads it and skips registered batches (audit row `skipped`,
   manifest outcome `already_loaded`), and `--force` bypasses it. Success
   audit rows upsert onto the index (re-runs refresh run id/timings instead
   of duplicating); `failed` rows accumulate as history; a CHECK pins
   statuses to `success|failed|skipped`.
5. **Reconciliation is bronze-vs-silver, not audit-vs-silver.** For every
   table whose batches are all registry-success, the loader re-reads the
   promoted Parquet parts, counts *distinct conflict keys per source*, and
   compares the sum with `count(*)` in Silver (`balanced` per table in the
   manifest; any imbalance fails the run with exit 1). Audit sums would only
   echo what the loader believes; comparing against Bronze independently
   detects lost rows, cross-source key collisions and registrations that
   outlived their data.
6. **Conversion is target-type-driven and loud.** `silver.convert` maps each
   value to the declared PostgreSQL type (unboxing numpy/pandas scalars,
   missing markers → `NULL`); anything unrepresentable raises `SilverLoadError`
   and fails the batch. There is no coercion path — the same philosophy as
   the Bronze standardizer (`"not-a-number"` never becomes `0`).
7. **Silver adds structure, not business rules.** No Great Expectations run
   here: quality gates once, at Bronze, on the data that is allowed to move
   (ADR-004). Silver contributes only what the database must enforce anyway
   (keys, FKs, `NOT NULL`, structural checks) plus run-time verification
   (rowcount cross-checks, reconciliation). Business-rule tests belong to dbt in Phase 10.

## Why not the alternatives

| Alternative | Why not chosen |
|-------------|----------------|
| Hand-written migration SQL for the schema | The 14 contracts are the single source of truth for columns/types/nullability; duplicated DDL would drift silently the next time a contract evolves. |
| One table per weather source (`weather_history`, `weather_forecast`) | Duplicates region/timezone context and forces downstream consumers (dbt, BI) to union the same entity twice; the merged table + per-source conflict keys keeps one entity, one count, one reconciliation entry. |
| Surrogate PK for `weather` (`weather_id BIGSERIAL`) | Hides natural identity — duplicates would load happily and reconciliation (which counts distinct natural keys) could never detect them. |
| `COPY` straight into `silver` | Key and FK maintenance would be paid twice (during `COPY` and again on every upsert) and a partially loaded batch would be visible mid-transaction; the staging heap gives one clean bulk write plus a staged-vs-inserted rowcount cross-check. |
| Append-only (`INSERT` with `ON CONFLICT DO NOTHING`) or truncate-and-rewrite per table | `DO NOTHING` silently drops updated dimension rows; full-table rewrite makes a one-batch refresh cost the whole table. `DO UPDATE` gives idempotent, batch-sized convergence (the overlapping `regions` batches live-test this). |
| Separate registry/state table (or file-based state) | Duplicates what `load_audit` already records per attempt; a second store can disagree with the audit trail. The partial unique success index makes the invariant unrepresentable instead of merely checked. |
| Treat `count(*)` of the target table as the idempotence check | Row counts cannot identify batches (overlaps, multi-source tables), and a failed half-load can coincidentally match a count. |
| Abort the run on the first batch failure | One bad batch (FK typo in a hand-crafted marker, corrupt part) would block all other sources; per-batch transactions already isolate the damage, and the failed record + exit 1 keep the run honest. |
| Server-side `COPY FROM 'file'` / `pg_dump`-style loading | Requires server-side file access (container volume plumbing) or superuser rights; psycopg text-mode `COPY FROM STDIN` works over the ordinary connection with the ordinary role. |
| Re-run Great Expectations at Silver | Two implementations of the same rules diverge; the data was gated at promotion time and Silver re-verifies it structurally (keys, FKs, rowcounts) without redefining validity. |
| Keep everything in Parquet and query via DuckDB/Arrow | Out of the stack's decision: PostgreSQL 16 is the designated warehouse engine (Phase 03), dbt-postgres is the planned transformation layer, and Superset expects a SQL database. |

## Consequences

- **Positive:** the whole Bronze → Silver path is one command
  (`apps.ingestion load`) with honest exit codes; reruns are near-free
  (`already_loaded`), `--force` repairs drift idempotently, and every attempt
  is queryable in `audit.load_audit` (`'failed'` rows explain themselves);
  reconciliation gives an independent, per-table `balanced` verdict recorded
  in `reports/silver/<run_id>.json`; DDL tracks contract evolution for free;
  integration tests run the real loader against a real PostgreSQL 16 in
  throwaway schemas (happy/idempotence/force/quality-exclusion/constraints/
  FK-rollback).
- **Negative:** reconciliation re-reads every promoted Parquet part on each
  run (measured: ~12 s of the 606 s medium run — acceptable now, would need a
  cheaper strategy if batches grow 100×); `TRUNCATE` staging makes the loader
  single-writer per schema (concurrent loads would need per-run staging
  names); `build_table_specs` fails hard when a contract lacks a silver
  mapping or uses an unsupported type — adding a source is now a two-place
  change (`SOURCE_TABLES` + contracts) until that mapping moves into the
  contracts themselves; the `weather` merge encodes a union-by-convention
  (two partial unique indexes) that a future third weather source would have
  to extend deliberately; audit `failed` rows grow unboundedly (compaction
  belongs with the planned observability work).
- **Follow-ups:** dbt (Phase 10) consumes `silver.*` as its staging source
  and adds business-rule tests; move the source→table mapping into contract
  metadata when the contract schema next evolves; wire `load` into the
  Airflow DAG (Phase 11) after `bronze_build`; consider partitioning
  `sales`/`purchases`/`inventory` if the warehouse outgrows single-node
  upserts; refresh-markers whose quality status changes must remain
  trustworthy — Silver only ever reads `_promoted/`.
