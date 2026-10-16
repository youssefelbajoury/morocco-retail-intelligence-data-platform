# ADR-007 — Star schema warehouse: Python-built dims/facts, identity SKs, rebuild-with-reconciliation

- **Status:** Accepted
- **Date:** 2026-10-08
- **Phase:** 09 — star schema data warehouse (7 dimensions, 3 facts)
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phase 08 left the platform with a live Silver layer in PostgreSQL 16
(`silver.*`, 13 contract-shaped tables with structural constraints, an audit
registry and bronze-vs-silver reconciliation). Superset and every dashboard
decision downstream need a **Kimball-style serving model** on top of it —
fact tables with documented grains, conformed dimensions, surrogate keys and
measures that reconcile back to Silver exactly. Five questions had to be
settled:

1. **What builds the star schema, and why not dbt?** The original roadmap put
   dbt at Phase 09. The phase directive rescope it: the warehouse must be
   implemented *now*, from Python, with dbt's project structure deferred to
   the next phase so the two are never half-done at once.
2. **What are the surrogate keys, and what makes a rebuild reproducible?**
   BI needs integer keys that stay stable across reloads — dashboards and
   tests must not silently re-point at different members because row order
   happened to change.
3. **What is the grain of each fact — and what stays off the fact?**
   Source profiling (Phase 09) showed `sale_id` unique across 2 000 rows,
   `purchase_id` unique across 938, and the inventory snapshot key
   `(date, store, product)` unique across 4 361 — three different grains, one
   of them a composite. Every non-measure column had to earn its place.
4. **Full rebuild or incremental load?** The dev dataset is small
   (thousands of rows), Silver itself is truncated/reloaded per run, and a
   warehouse that silently keeps rows from a *previous* Silver snapshot is
   worse than one that is rebuilt honestly.
5. **What does the database enforce vs. what belongs to tests/BI?**
   Orphans, duplicates and illegal values must be *unrepresentable*, but
   ratio columns, slowly-changing history and dashboards are other layers'
   work.

## Decision

1. **Ten tables, all derived from live Silver — no new data sources.**
   Dimensions `dim_region`, `dim_canal`, `dim_date`, `dim_magasin`,
   `dim_client`, `dim_produit`, `dim_promotion`; facts `fact_ventes`
   (sale line item), `fact_achats` (receipt line), `fact_stock`
   (`date × store × product` snapshot). `dim_date`, `dim_magasin`,
   `dim_produit` are conformed across all three facts; `dim_client`,
   `dim_canal`, `dim_promotion` serve `fact_ventes`.
2. **Phase 09 ships a Python builder (`mri.warehouse`); dbt takes the same
   schema over in Phase 10.** One phase, one owner: the builder emits 51
   declarative statements (schema/tables/constraints/indexes, one truncate,
   10 `INSERT … SELECT` loads) from the `TABLES` contract in
   `schema.py` — the same structure dbt will later express as models.
3. **Surrogate keys are `GENERATED ALWAYS AS IDENTITY` on exactly the seven
   dimension PKs; `date_sk` is a uniform identity, not `yyyymmdd`.**
   Reproducibility comes from mechanics, not from encoding: every build runs
   `TRUNCATE … RESTART IDENTITY CASCADE` and inserts dimensions `ORDER BY`
   their `UNIQUE` business key inside one transaction, so an identical Silver
   snapshot always yields identical `*_sk` values (integration test rebuilds
   and compares).
4. **The warehouse is rebuilt, not incrementally merged.** One outer
   transaction: DDL (idempotent) → truncate → dims in dependency order →
   facts → reconciliation. A failed build rolls back completely; the schema
   is never half-built, and no row can outlive its Silver source.
5. **No-fabrication column policy.** Every column is a verbatim Silver
   column, a `*_sk` key, or one declared derived column
   (`dim_date.day = EXTRACT(DAY FROM date)`). Source identifiers without a
   dimension stay **degenerate** (`transaction_id`, `hour_of_day`,
   `stock_status`). Additive measures only — `margin_mad` is stored (it is a
   source column), but no ratio/growth column is computed in SQL.
6. **Constraints mirror Silver, named and idempotent.** PKs and business-key
   `UNIQUE`s on all tables; 18 FKs (facts → dims, plus dimension-to-dimension
   links) as self-healing `DO $$ … pg_constraint` blocks; inline `CHECK`s for
   enums/ranges already guaranteed by Silver (e.g. `quantity ≥ 1`,
   `0 ≤ discount ≤ 1`, `holiday_name` ⇔ `is_holiday`). Optional keys
   (`client_sk`, `promotion_sk`, promotion's region/channel) stay nullable —
   Silver itself permits those rows, so rejecting them would fabricate a
   stricter business than the source has.
7. **Eleven secondary indexes, all on fact FK columns.**
   `ix_fact_ventes_*` (6), `ix_fact_achats_date_sk|magasin_sk|produit_sk`
   (3), `ix_fact_stock_magasin_sk|produit_sk` (2). `fact_stock.date_sk` is
   covered by the leading PK column; dimensions are probed through their
   PK/UNIQUE indexes; `create_index` is a per-FK flag in the contract.
8. **Reconciliation = row counts for all 10 tables + per-fact measure sums
   vs Silver** (money as `round(sum(x::numeric), 2)`, integers exact). Any
   imbalance ⇒ `ok=False` ⇒ CLI exit 1. No manifest files — the database
   *is* the artifact; dbt (Phase 10) owns its own manifest story.
9. **CLI contract:** `python -m apps.warehouse [--json] [--schema NAME]
   build|plan`; exit `0` balanced / `1` build-or-balance failure /
   `2` usage. `plan` prints all statements without touching the database.

## Why not the alternatives

| Alternative | Why not chosen |
|-------------|----------------|
| Let dbt build the star schema in Phase 09 | Two changes at once: the phase directive scopes the warehouse to Python now and dbt's project structure next — a half-configured dbt project and an unbuilt warehouse would both be unfinished. The `TABLES` contract keeps one declarative source both layers can inherit. |
| Snowflake (denormalized) schema | The facts share conformed dimensions by design; snowflaking (`dim_magasin → dim_region` normalization *inside* the model) would buy nothing — regions are already a separate table, and star joins match Superset's implicit-join model. |
| `date_sk = yyyymmdd` integer | Feels deterministic but encodes a business rule inside the key, breaks the uniform identity convention, and rejects valid dates outside 1000–9999 for zero benefit; correctness is proven by FK coverage tests, not by key shape. |
| `BIGSERIAL` / default sequences instead of `IDENTITY` | Same keys, weaker story: `IDENTITY` is attached to exactly the column, `TRUNCATE … RESTART IDENTITY` is the documented reset, and `GENERATED ALWAYS` blocks accidental manual inserts. |
| Keep natural keys as PKs (no surrogate keys) | Facts would repeat multi-column text keys everywhere (index bloat, awkward joins), and a re-keyed Silver dimension could silently change history — surrogates isolate the model from source churn. |
| Incremental `INSERT … ON CONFLICT` loads | Cannot detect rows that vanished from Silver (deleted/re-generated sources), so reconciliation would need tombstone logic; truncate-reload makes warehouse = f(Silver) a *function*, which tests can assert. |
| SCD-2 history now | Nothing in the requirements tracks slowly-changing attributes; SCD-2 would multiply rows per grain, break the "grain = PK" invariant, and demand snapshot bookkeeping dbt (next phase) is better placed to own. |
| Manifest/YAML files recording each load | The schema already declares grains, keys and measures in `schema.py`; a second copy in YAML would drift. Reconciliation results go to stdout/JSON; persistence belongs with the observability phase. |
| Index every FK including dimension internals | Dimensions hold tens-to-hundreds of rows — sequential scans beat index probes; the `create_index=False` flag also documents why `fact_stock.date_sk` needs nothing beyond its composite PK. |
| Compute `margin_pct`, growth, conversion in SQL | Non-additive/arithmetic conveniences belong to BI queries or dbt models; storing them invites double-definition and stale derived state. |
| Separate warehouse engine (DuckDB, clickhouse) | Stack decision since Phase 03: PostgreSQL 16 is the one relational engine; Superset and dbt-postgres target it; a second engine doubles the local footprint for no current scale need. |
| Non-idempotent `ALTER TABLE ADD CONSTRAINT` | Re-running the builder (or running it against a database someone touched by hand) would fail; self-healing `DO` blocks + `IF NOT EXISTS` make every statement re-runnable. |

## Consequences

- **Positive:** one command (`python -m apps.warehouse build`) produces a
  fully constrained, index-backed, reconciled star schema with honest exit
  codes; identical Silver ⇒ identical SKs, so downstream tests/dashboards are
  stable; the `TABLES` contract gives unit tests (grain/keys/no-fabrication/
  DDL shape) and documentation a single source of truth; integration tests
  prove the schema against a real PostgreSQL (reconciliation, orphan sweep,
  constraint rejection, rebuild determinism); dbt inherits a defined target
  instead of inventing one.
- **Negative:** rebuild semantics mean the warehouse is only as current as
  the last `build` (no streaming/incremental freshness — acceptable at
  batch cadence, needs rework if Silver becomes append-only); `TRUNCATE`
  makes the warehouse single-writer (concurrent builds would serialize);
  translating this Python contract into dbt models next phase is deliberate
  duplication for one transition period; the no-fabrication policy means
  every new derived column requires a contract + ADR edit, which is friction
  by design; Silver's `float8` money columns propagate their rounding
  (`1e-13` diffs) — exact decimals would need a Silver-level change.
- **Follow-ups:** dbt (Phase 10) re-expresses these tables as
  `marts` models with tests and owns future schema evolution; Airflow (later)
  schedules `build` after the Silver load; Superset consumes the schema
  as-is; consider `numeric` money types and a facts partitioning strategy only
  if scale demands; `dim_weather`/`dim_exchange_rate` wait for a consumer
  that actually needs them.
