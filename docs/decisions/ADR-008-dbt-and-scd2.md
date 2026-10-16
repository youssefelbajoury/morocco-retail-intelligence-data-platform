# ADR-008 — dbt owns the analytics layer; SCD type 2 for `dim_produit` only

- **Status:** Accepted
- **Date:** 2026-10-08
- **Phase:** 10 — dbt analytics layer + SCD type 2
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phase 09 delivered the star schema as a Python-built reference
([ADR-007](ADR-007-star-schema.md)) and explicitly deferred two things to
Phase 10: the dbt project and SCD-2 history ("SCD-2 history now … dbt (next
phase) is better placed to own it"). Phase 10 had to settle five questions:

1. **How does dbt take over without invalidating Phase 09?** Replacing the
   Python warehouse outright would discard a reconciled, tested artefact;
   ignoring it would leave two star schemas drifting apart with no way to
   tell which is right.
2. **Which layer does dbt actually build?** The roadmap said
   "intermediate, marts, star schema" — but Silver is Python-owned, and
   inventing a second cleaning layer on top of an already-constrained Silver
   would duplicate Phase 08's job.
3. **Why SCD type 2 at all, and on what?** Product attributes (brand, price,
   cost, margin, category, active flag) change in the source while the
   business key does not; historical facts would otherwise silently join the
   *current* attributes. But applying SCD2 everywhere multiplies every grain
   for dimensions that never change.
4. **How is history captured — snapshot or hand-rolled model?** The project
   has no `updated_at` columns (fabricating provenance was rejected in
   ADR-007's no-fabrication policy), and a hand-rolled slowly-changing model
   re-implements well-trodden machinery poorly.
5. **What proves the two implementations agree?** "Both exist" is only
   acceptable if equivalence is asserted by tests, not by hope.

## Decision

1. **dbt is the authoritative analytics layer; the Python warehouse stays as
   the reference implementation of the Phase 09 contract.** Both are built
   from live Silver; neither feeds the other. A guarded singular test
   (`assert_marts_match_warehouse`) row-compares `fact_ventes` (including
   `product_id` via dimension joins) and row-count-compares the other nine
   marts whenever `warehouse_schema` exists — and compiles to a no-op when it
   does not, so dbt alone always builds.
2. **dbt models cover staging → intermediate → marts, and nothing Python
   already owns.** 10 `stg_*` models re-type/clean Silver 1:1 (explicit
   casts, `nullif(trim(...))` ids, keeps `batch_id`/`ingestion_timestamp`),
   6 `int_*` models unify `event_date` and drop provenance, 10 marts models
   rebuild the star schema. No column is fabricated at any layer.
3. **SCD type 2 for `dim_produit` only.** The nine attribute columns are
   tracked with a dbt **`check`-strategy snapshot**
   (`snapshot_dim_produit`, `unique_key=product_id`); `dim_produit` reshapes
   it into half-open `[valid_from, valid_to)` versions with
   `is_current` and a deterministic hash `produit_sk`
   (`text_hash_sk(product_id|version_no|valid_from)`). Facts join it
   **as of the event date**, so historical events resolve to the attributes
   that were true then while their surrogate keys stay stable. All other
   dimensions stay current-state, with the per-dimension justification
   recorded in [`docs/data-model/scd2.md`](../data-model/scd2.md).
4. **Capture with dbt's built-in snapshot machinery, not a custom model.**
   `check` strategy compares the tracked columns directly — no `updated_at`
   needed, none fabricated. The snapshot runs inside `dbt build`, is
   idempotent, and lives in the isolated `dbt_snapshots` schema (per-run
   suffix via the `schema_suffix` var).
5. **Surrogate-key parity is engineered, not assumed.** Non-SCD2 dimensions
   use `row_number() OVER (ORDER BY business key)`, which reproduces the
   Python builder's identity keys exactly (its inserts are `ORDER BY`
   business key); `dim_produit` keys are version-derived and compared
   current-rows-only. Same Silver snapshot ⇒ same keys ⇒ row-level equality
   is even checkable.
6. **Everything runs through `python -m apps.dbt`.** The wrapper injects
   `DBT_TARGET_*` from the repository's `Settings` (never overriding an
   explicit value), pins `--project-dir/--profiles-dir`, surfaces a missing
   password as exit 1 with a clear message, and forwards dbt's exit code —
   so pytest, integration tests and humans all exercise the identical path.

## Why not the alternatives

| Alternative | Why not chosen |
|-------------|----------------|
| Drop the Python warehouse once dbt works | Phase 09's builder is the tested reference for constraints, reconciliation and SK determinism; deleting it would make the parity test impossible and turn "dbt is right" into an unverifiable claim. Two implementations with an assertion beat one implementation with a hope. |
| Let dbt read Bronze or raw files instead of Silver | Duplicates Phase 08's cleaning/constraints inside SQL, forks the definition of "trusted data", and loses Silver's audit registry. dbt starts where Python's contract ends. |
| SCD2 on every dimension | Multiplies row counts for dimensions with no churn (`dim_region` has 12 rows), breaks the `grain = PK` invariant facts rely on, and forces snapshot bookkeeping into joins nobody asked for. |
| SCD1 / silent overwrite for products | Historical facts would join *today's* brand/price — quietly wrong analytics (a June sale priced at October's price). The whole point of the phase is preserving history. |
| Hand-rolled SCD2 model (row_number + lag over Silver) | Re-implements change detection, version bookkeeping and idempotent updates that dbt snapshots already do; would also need its own state table — i.e. the same machinery, worse tested. |
| `timestamp` strategy for the snapshot | Requires an `updated_at` column Silver does not have; deriving one from load time would fabricate source provenance (violates the ADR-007 no-fabrication policy). `check` compares the actual business columns instead. |
| Make `dim_produit`'s key an identity sequence like the other dims | Version history must be append-only and rebuild-stable; a sequence makes keys depend on insert order across runs, while `text_hash_sk(product_id|version_no|valid_from)` is deterministic from the data itself. |
| Join facts to the *current* product row and keep history only in the dim | Facts would carry today's attributes onto yesterday's events — the exact bug SCD2 exists to prevent. |
| Prove equivalence by row counts only | Counts pass while values drift. The parity test compares `fact_ventes` row by row (all measures, SKs and `product_id`), so a semantic divergence fails the build. |
| Run dbt bare (`cd dbt && dbt build`) | Loses the repository's env handling, project/profile pinning and uniform exit codes; tests and CI would exercise a different path than developers. |

## Consequences

- **Positive:** one command (`python -m apps.dbt build`) compiles, runs and
  tests the entire analytics layer — 167 nodes green on dev, 140 tests
  including the warehouse parity assertion; product history is queryable and
  provable (single-current + contiguity tests), and a live change scenario
  confirmed versions open correctly while fact keys stay stable; freshness
  (`warn 7d / error 30d` over `ingestion_timestamp`) gives the pipeline a
  data-lag signal dbt reports natively; `docs generate` + `persist_docs`
  produce a lineage/catalog artifact for the BI phase; schema-suffix
  isolation lets integration tests build the whole project without touching
  dev state.
- **Negative:** the star schema now exists twice by design — a deliberate,
  test-enforced duplication that must be maintained in lockstep (schema
  changes need edits in `src/mri/warehouse/schema.py` *and* the marts
  models, or parity fails — which is the point, but it is friction);
  `dim_produit` semantics differ from the Python build (versions vs
  current-state), so consumers must know which schema they query;
  snapshots are stateful tables — a dropped snapshot resets history (the
  documented reset path), and a stale snapshot on an empty Silver would not
  self-heal without an explicit rebuild; dbt's `target/`/`logs/` and the
  test `dbt_test__audit` schema add generated artifacts to keep out of git;
  full-rebuild materialization (no incremental facts yet) is fine at dev
  scale but must be revisited before large facts.
- **Follow-ups:** Airflow (Phase 11) should orchestrate
  `apps.ingestion load → apps.warehouse build → python -m apps.dbt build` as
  one unit so the parity test always sees both schemas; Superset (Phase 12)
  consumes the dbt marts (`dbt_marts`) as primary, with `warehouse` as the
  audited fallback; incremental materialization for facts and SCD2 for other
  dimensions only if a consumer actually needs them; CI (Phase 13) runs
  `ruff → mypy → pytest → apps.dbt build` against a service PostgreSQL.
