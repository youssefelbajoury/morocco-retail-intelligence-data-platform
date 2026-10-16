# Facts & Dimensions — Column Reference

> **Status: IMPLEMENTED (Phase 09).** Companion to
> [`star-schema.md`](star-schema.md): one section per warehouse table — grain,
> keys, columns grouped by role, joined dimensions, source and notes.
> Machine-readable source of truth: `TABLES` in
> [`src/mri/warehouse/schema.py`](../../src/mri/warehouse/schema.py).

Column roles: **SK** = surrogate key, **FK** = surrogate foreign key,
**BK** = business key (natural source identifier, `UNIQUE`),
**A** = attribute (verbatim Silver column), **M** = measure (numeric),
**D** = degenerate dimension (business identifier / classification without a
dimension table), **derived** = computed in SQL (only `dim_date.day`).

---

## Dimensions

### `dim_region`

| | |
|---|---|
| Grain | one row per Moroccan region |
| SK / BK | `region_sk` / `region_id` |
| Source | `silver.regions` (12 rows in dev dataset) |
| Dimensions joined | none (top of the hierarchy) |

| Column | Role | Type | Constraint |
|--------|------|------|------------|
| `region_sk` | SK | BIGINT | PK, IDENTITY |
| `region_id` | BK/A | TEXT | UNIQUE, NOT NULL |
| `region_name` | A | TEXT | NOT NULL |
| `demand_weight` | A | DOUBLE PRECISION | 0 ≤ x ≤ 1 |

Note: `dim_magasin`, `dim_client` and (optionally) `dim_promotion` carry
`region_sk`, so region roll-ups work across every fact.

### `dim_canal`

| | |
|---|---|
| Grain | one row per sales channel (`physical` / `digital`) |
| SK / BK | `canal_sk` / `channel_id` |
| Source | `silver.channels` (3 rows) |

| Column | Role | Constraint |
|--------|------|-----------|
| `canal_sk` | SK, PK, IDENTITY | |
| `channel_id` | BK/A | UNIQUE |
| `channel_name` | A | |
| `channel_type` | A | `IN ('physical', 'digital')` |
| `demand_weight` | A | 0 ≤ x ≤ 1 |

### `dim_date`

| | |
|---|---|
| Grain | one row per calendar date, 2021-01-01 → 2024-12-31 (1 461 rows) |
| SK / BK | `date_sk` / `date` |
| Source | `silver.calendar` (deterministic generator — Ramadan/Eid/holiday flags are never invented here) |
| Dimensions joined | conformed: all 3 facts |

| Column | Role | Constraint |
|--------|------|-----------|
| `date_sk` | SK, PK, IDENTITY | uniform integer key, not `yyyymmdd` (ADR-007) |
| `date` | BK/A, DATE | UNIQUE |
| `day` | **derived** | 1–31 (`EXTRACT(DAY FROM date)`) |
| `year`, `month`, `quarter`, `iso_week`, `weekday`, `weekday_name` | A | range checks |
| `is_weekend` | A | `= (weekday >= 5)` |
| `is_holiday`, `holiday_name` | A | `holiday_name` NOT NULL ⇔ `is_holiday` |
| `is_ramadan`, `is_eid_al_fitr`, `is_eid_al_adha` | A | |
| `season` | A | `IN ('winter','spring','summer','autumn')` |

### `dim_magasin`

| | |
|---|---|
| Grain | one row per retail store |
| SK / BK | `magasin_sk` / `store_id` |
| Source | `silver.stores` (4 rows) |
| Dimensions joined | `dim_region` via `region_sk` (INNER — every store belongs to a region) |

| Column | Role | Constraint |
|--------|------|-----------|
| `magasin_sk` | SK, PK, IDENTITY | |
| `store_id` | BK/A | UNIQUE |
| `store_name`, `city` | A | |
| `region_id` | A | source key retained for lineage |
| `region_sk` | FK → `dim_region` | |
| `store_format` | A | `IN ('hypermarket','supermarket','convenience','cash_and_carry')` |
| `size_sqm` | A | > 0 |
| `open_date`, `is_active` | A | |

### `dim_client`

| | |
|---|---|
| Grain | one row per customer |
| SK / BK | `client_sk` / `customer_id` |
| Source | `silver.customers` (200 rows) |
| Dimensions joined | `dim_region` (INNER), `dim_canal` (INNER, preferred channel) |

| Column | Role | Constraint |
|--------|------|-----------|
| `client_sk` | SK, PK, IDENTITY | |
| `customer_id` | BK/A | UNIQUE |
| `signup_date` | A | |
| `region_id`, `region_sk` | A + FK | |
| `birth_year` | A | 1900–2024 |
| `gender` | A | `IN ('F','M','U')` |
| `segment` | A | `IN ('jeune','famille','senior','actif')` |
| `loyalty_tier` | A | `IN ('bronze','silver','gold','platinum')` |
| `preferred_channel_id`, `canal_sk` | A + FK | |
| `is_active` | A | |

### `dim_produit`

| | |
|---|---|
| Grain | one row per product |
| SK / BK | `produit_sk` / `product_id` |
| Source | `silver.products` (60 rows) |
| Dimensions joined | none (conformed: all 3 facts) |

| Column | Role | Constraint |
|--------|------|-----------|
| `produit_sk` | SK, PK, IDENTITY | |
| `product_id` | BK/A | UNIQUE |
| `product_name`, `category`, `subcategory`, `brand` | A | |
| `unit_cost_mad` | A | > 0 |
| `unit_price_mad` | A | ≥ `unit_cost_mad` |
| `margin_pct` | A | 0 ≤ x < 1 |
| `launch_date`, `is_active` | A | |

### `dim_promotion`

| | |
|---|---|
| Grain | one row per promotion |
| SK / BK | `promotion_sk` / `promotion_id` |
| Source | `silver.promotions` (20 rows) |
| Dimensions joined | `dim_produit` (INNER — every promotion targets a product), `dim_region` + `dim_canal` (LEFT — national promotions have no region/channel) |

| Column | Role | Constraint |
|--------|------|-----------|
| `promotion_sk` | SK, PK, IDENTITY | |
| `promotion_id` | BK/A | UNIQUE |
| `product_id`, `produit_sk` | A + FK | |
| `region_id`, `region_sk` | A + FK | both nullable |
| `channel_id`, `canal_sk` | A + FK | both nullable |
| `promotion_type` | A | `IN ('remise','prix_promo','bogo','fidelite','bundle')` |
| `discount_pct` | A | 0 ≤ x ≤ 1 |
| `start_date`, `end_date` | A | `end_date ≥ start_date` |

---

## Facts

### `fact_ventes`

| | |
|---|---|
| **Grain** | one row per **sale line item** — one product line inside one shopping transaction (`sale_id` unique; `transaction_id` groups the lines of one basket) |
| Business key | `sale_id` (PK) |
| Source | `silver.sales` (2 000 rows) |
| Load order | 1st fact (after all 7 dimensions) |

**Keys**

| Column | Join | Type |
|--------|------|------|
| `date_sk` | → `dim_date` | INNER |
| `magasin_sk` | → `dim_magasin` | INNER |
| `canal_sk` | → `dim_canal` | INNER |
| `produit_sk` | → `dim_produit` | INNER |
| `client_sk` | → `dim_client` | **LEFT — nullable** (anonymous sales: 180/2 000) |
| `promotion_sk` | → `dim_promotion` | **LEFT — nullable** (no active promo: 1 997/2 000) |

**Columns**

| Column | Role | Constraint |
|--------|------|-----------|
| `sale_id` | BK, PK | UNIQUE by PK |
| `transaction_id` | D | grouping id of the basket, kept as degenerate column |
| `hour_of_day` | D/A | 8–23 |
| `quantity` | M | ≥ 1 |
| `unit_price_mad` | M/A | > 0 |
| `discount_pct` | M/A | 0 ≤ x ≤ 1 |
| `net_amount_mad` | M | ≥ 0; `≈ qty × price × (1 − discount)` |
| `cost_amount_mad` | M | ≥ 0 |
| `margin_mad` | M | ≥ 0; `= net − cost` |

Reconciled measures: `quantity`, `net_amount_mad`, `cost_amount_mad`,
`margin_mad`.

### `fact_achats`

| | |
|---|---|
| **Grain** | one row per **purchase receipt line** — one product received by one store on one date (`purchase_id` unique) |
| Business key | `purchase_id` (PK) |
| Source | `silver.purchases` (938 rows; no promotions/customers/channels in this stream) |

**Keys**: `date_sk`, `magasin_sk`, `produit_sk` — all INNER.

| Column | Role | Constraint |
|--------|------|-----------|
| `purchase_id` | BK, PK | |
| `quantity` | M | ≥ 1 |
| `unit_cost_mad` | M | > 0 |
| `total_cost_mad` | M | ≥ 0; `= qty × unit_cost` |

Reconciled measures: `quantity`, `total_cost_mad`.

### `fact_stock`

| | |
|---|---|
| **Grain** | one row per **stock snapshot** — one store's stock level for one product on one snapshot date; grain = `date_sk × magasin_sk × produit_sk` (composite PK **is** the grain) |
| Business key | `(date_sk, magasin_sk, produit_sk)` |
| Source | `silver.inventory` (4 361 rows) |

**Keys**: `date_sk` (INNER, covered by the leading PK column — no separate
index), `magasin_sk`, `produit_sk` (INNER, indexed).

| Column | Role | Constraint |
|--------|------|-----------|
| `quantity_on_hand` | M | ≥ 0 |
| `stock_value_mad` | M | ≥ 0 |
| `stock_status` | D | `IN ('in_stock','low','out_of_stock')` — snapshot classification, kept degenerate |

Reconciled measures: `quantity_on_hand`, `stock_value_mad`.

---

## Nullability summary (facts)

| Column | Nullable | Dev rows | Reason |
|--------|----------|----------|--------|
| `fact_ventes.client_sk` | yes | 180 / 2 000 | Silver allows `customer_id IS NULL` (walk-in sales) |
| `fact_ventes.promotion_sk` | yes | 1 997 / 2 000 | most sales run without an active promotion |
| all other fact FKs | no | 0 | inner joins; a miss would fail row-count reconciliation |

## What is intentionally absent

- No `dim_weather` / `dim_exchange_rate`: no required fact joins them today;
  they remain a serving-layer decision for a later phase (the Silver data is
  already stored).
- No SCD-2/history columns: rebuild-from-silver semantics (ADR-007); dbt
  (next phase) may introduce snapshots where history is needed.
- No computed ratios (`margin_pct` per sale, growth, velocity): BI-layer
  arithmetic, not warehouse measures.
