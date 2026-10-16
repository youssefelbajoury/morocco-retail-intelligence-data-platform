# SCD Type 2 — `dim_produit`

> **Status: IMPLEMENTED (Phase 10).** Product attributes are tracked with a
> slowly-changing dimension (SCD type 2): a dbt **snapshot**
> (`snapshot_dim_produit`, check strategy) records every attribute change, and
> the `dim_produit` mart exposes versioned rows with
> `valid_from`/`valid_to`/`is_current` and a deterministic surrogate key per
> version. Facts join it **as of the event date**, so history is queryable
> while fact keys stay stable. Every other dimension remains current-state
> (see [ADR-008](../decisions/ADR-008-dbt-and-scd2.md)).

---

## 1. Why only products get SCD2

Products are the one Silver entity whose *attributes* change while its
business key does not — brand, price, cost, margin, category, active flag
(all nine `check_cols`) are editable in the source generator and matter
historically: a sale last June happened at last June's price and brand.
The other dimensions are stable or effectively snapshot-only:

| Dimension | Why current-state is correct |
|-----------|------------------------------|
| `dim_date`, `dim_region`, `dim_canal` | effectively static reference data |
| `dim_magasin` | 4 stores, no attribute churn in the synthetic source |
| `dim_client` | behavioural history lives in the facts, not in profile edits |
| `dim_promotion` | promotions are already time-bounded rows (start/end), not versions |

SCD2 on everything would multiply every grain, break `grain = PK` invariants
and add bookkeeping with no consumer — documented in
[ADR-008](../decisions/ADR-008-dbt-and-scd2.md).

---

## 2. Capture — the snapshot

`dbt/snapshots/snapshot_dim_produit.sql`:

```sql
{% snapshot snapshot_dim_produit %}
{{ config(
    unique_key="product_id",
    strategy="check",
    check_cols=[ "product_name", "category", "subcategory", "brand",
                 "unit_cost_mad", "unit_price_mad", "margin_pct",
                 "launch_date", "is_active" ] ) }}
select product_id, product_name, category, subcategory, brand,
       unit_cost_mad, unit_price_mad, margin_pct, launch_date, is_active
from {{ ref('int_products') }}
{% endsnapshot %}
```

- **Strategy `check`** (not `timestamp`): `int_products` carries no
  `updated_at` column, and inventing one would fabricate provenance the
  source does not have. `check` compares the nine tracked columns and opens a
  new version exactly when one of them changes.
- **`unique_key = product_id`** — one version chain per product.
- State lives in `dbt_snapshots.snapshot_dim_produit` and is maintained by
  `dbt snapshot` / any `dbt build`; it survives re-runs (idempotent) and is
  rebuilt from scratch only if the table is dropped.
- Output bookkeeping columns: `dbt_valid_from`, `dbt_valid_to`,
  `dbt_updated_at`, `dbt_change_type`, `dbt_scd_id`.

---

## 3. Exposure — the `dim_produit` mart

`dbt/models/marts/dim_produit.sql` reshapes snapshot rows into serving-model
shape:

1. `versions` — `row_number() OVER (PARTITION BY product_id ORDER BY
   dbt_valid_from)` gives `version_no`.
2. `effective` — business-effective dates:
   - `valid_from` = `launch_date` for **version 1** (the attribute set has
     been true since launch; the snapshot only learns about it at first
     capture), `dbt_valid_from` for every later version;
   - `valid_to` = `dbt_valid_to` (`null` for the current version).
3. Final select:

```sql
{{ text_hash_sk("product_id || '|' || version_no || '|' || valid_from::text") }}
    as produit_sk,
… valid_from, valid_to, valid_to is null as is_current
```

- **`produit_sk`** is a deterministic 64-bit signed hash of
  `product_id | version_no | valid_from` — stable across rebuilds (no
  sequence dependence), unique per version, and deliberately *not* derived
  from current-ness: reopening or adding versions never rewrites old keys.
- Current-state consumers (e.g. `dim_client` parity comparisons) filter
  `is_current`.

Dev state after the validation run: **60 version rows / 46 distinct products**
(14 products have exactly one version, `is_current` everywhere).

---

## 4. Consumption — as-of joins in the facts

All three facts join the SCD2 dimension **as of the event date**:

```sql
join {{ ref('dim_produit') }} p
    on p.product_id = s.product_id
    and s.event_date >= p.valid_from
    and (p.valid_to is null or s.event_date < p.valid_to)
```

Consequences:

- A sale on `2026-06-01` resolves to the product attributes effective on
  `2026-06-01` — not today's.
- Each fact row matches **exactly one** version (versions are contiguous and
  half-open `[valid_from, valid_to)` — enforced by the singular tests), so
  the join can never fan out.
- `fact_*` keys (`produit_sk`) reference the *version* that was current at
  event time; they are therefore historical by construction.

---

## 5. Invariants (enforced in `dbt build`)

| Test | Guarantees |
|------|------------|
| `assert_scd2_single_current` | exactly one `valid_to is null` row per `product_id` |
| `assert_scd2_contiguous_versions` | `valid_to` of version *n* = `valid_from` of version *n+1* (no gaps, no overlaps) |
| generic `unique` / `not_null` on `dim_produit` | `produit_sk` unique, keys present |
| `relationships` (3 fact tests) | every fact `produit_sk` exists in `dim_produit` |
| `assert_fact_row_counts` | fact row counts unchanged vs Silver (as-of join never drops/duplicates) |

---

## 6. Live change scenario (validated on dev, then reverted)

End-to-end proof that history is preserved when a product changes:

1. **Change:** brand of `P000035` flipped `Riad → AtlasFreshTest` in Silver.
2. **`python -m apps.dbt build`:**
   - snapshot opened a **new version**: 60 → **61 rows / 60 current**;
   - version 1: `valid_from = 2018-02-20` (launch_date),
     `valid_to = 2026-10-08`, `is_current = false`;
   - version 2: `valid_from = 2026-10-08`, `valid_to = null`,
     `is_current = true`, brand `AtlasFreshTest`;
   - contiguity exact, **0** single-current violations;
   - `fact_ventes` for that product **kept its pre-change `produit_sk`**
     (signature `394|580ce6f2bc72af6215c9508fefe7a183` unchanged) —
     historical events stay on version 1, new events will use version 2.
3. **Revert:** brand restored in Silver, `dbt_snapshots.snapshot_dim_produit`
   dropped, `python -m apps.dbt build` re-ran from scratch → 60 versions,
   **167/167 PASS** — a clean reset path when the source is regenerated.

---

## 7. Operational notes

- **Run:** the snapshot executes inside every `python -m apps.dbt build`
  (before marts), so attribute changes are captured on each pipeline run.
- **Reset:** dropping `dbt_snapshots.snapshot_dim_produit` and rebuilding
  replays history from the current Silver state (used by tests/scenario).
- **Schema isolation:** with `--vars '{schema_suffix: _it1}'` the snapshot
  lands in `dbt_snapshots_it1`, so integration runs never touch dev state.
- **Parity:** the warehouse parity test compares `dim_produit`
  **current rows only** against the Phase 09 Python build (which is
  current-state by design); version rows are dbt-only history.

---

## 8. Related documents

- [`dbt.md`](dbt.md) — the full dbt layer (lineage, tests, validation).
- [`../decisions/ADR-008-dbt-and-scd2.md`](../decisions/ADR-008-dbt-and-scd2.md)
  — why SCD2 is dbt's job and why only `dim_produit`.
- [`star-schema.md`](star-schema.md) — the Phase 09 reference (current-state
  `dim_produit`).
