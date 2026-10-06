# Synthetic Data Model

**Status: IMPLEMENTED (PHASE 02) · controlled synthetic data, not real enterprise data.**

The synthetic data generator produces a deterministic, reproducible, multi-year
Moroccan retail/distribution dataset used to develop and test the platform
end-to-end before public API ingestion is added.

> **Honesty statement.** The dataset is *controlled synthetic data*. Product
> popularity, regional demand, store differences, loyalty-driven purchase
> frequency, seasonality (weekend / holiday / Ramadan / Aïd), promotion exposure
> with quantity lift, realistic margins and rolling replenishment are modelled
> explicitly and are internally consistent. It is **not** statistically
> equivalent to a real enterprise and must never be presented as observed
> production data.

---

## 1. How to run

```bash
# From the repository root (uv-managed Python 3.12)
uv run python -m apps.data_generator --scale medium --seed 42
uv run python -m apps.data_generator --scale small  --seed 42 --format parquet
uv run python -m apps.data_generator --scale tiny   --seed 7  --output data/generated
```

Options:

| Flag | Default | Meaning |
|------|---------|---------|
| `--scale` | `medium` | `tiny` \| `small` \| `medium` \| `large` \| `xl` |
| `--seed` | `42` | integer random seed (reproducibility) |
| `--output` | `data/generated` | output root |
| `--format` | `csv` | `csv` \| `parquet` |
| `--quiet` | off | suppress the count report |
| `--dq-fixture` | none | write a controlled data-quality fixture instead |
| `--dq-output` | `data/dq_fixtures` | fixture output root (never `data/generated`) |

Useful from Python:

```python
from mri.synthetic import generate, get_profile

tables = generate(seed=42, profile=get_profile("small"))  # dict[str, pandas.DataFrame]
```

---

## 2. Reproducibility (random seed)

- All randomness derives from `numpy.random.SeedSequence(seed)`, spawned once per
  **fixed component order**: `stores, products, customers, promotions, sales,
  purchases, inventory` (plus an internal `world` component).
- Sales are generated in chunks; each chunk gets its own child generator, so
  chunking does not change results for a fixed profile + seed.
- Running the same command twice with the same seed produces **byte-identical
  CSV files**. Each run's manifest records a SHA-256 per file; the test suite
  asserts this for `tiny` and the PHASE 02 validation compared all 10 `medium`
  files byte-for-byte across two independent runs.
- Changing `--seed` changes the logical dataset (asserted by tests).

---

## 3. Output layout

```
data/generated/
└── <scale>/
    ├── manifest.json
    ├── regions/regions.csv
    ├── channels/channels.csv
    ├── stores/stores.csv
    ├── products/products.csv
    ├── customers/customers.csv
    ├── promotions/promotions.csv
    ├── sales/sales.csv
    ├── purchases/purchases.csv
    ├── inventory/inventory.csv
    └── calendar/calendar.csv
```

The `<scale>/` level avoids one scale overwriting another. `manifest.json`
contains: generator name + version, profile, seed, format, period, target vs
actual `row_counts`, per-file `sha256`, column lists, and a `data_class` marker.
Generated data is git-ignored (`/data/*` in `.gitignore`) and is never committed.

---

## 4. Entities and columns

| Entity | Primary key | Notes |
|--------|-------------|-------|
| `regions` | `region_id` | 12 Moroccan regions + normalised `demand_weight` |
| `channels` | `channel_id` | In-store / E-commerce / Mobile app + `demand_weight` |
| `stores` | `store_id` | city, region, format, surface, open date, active flag |
| `products` | `product_id` | category, subcategory, brand, cost, price, margin, launch date |
| `customers` | `customer_id` | signup, region, birth year, gender, segment, loyalty tier, preferred channel |
| `promotions` | `promotion_id` | product + optional region/channel scope, type, discount, window |
| `sales` | `sale_id` | line-item fact (`transaction_id` groups a basket) |
| `purchases` | `purchase_id` | store→product replenishment receipts |
| `inventory` | `snapshot_date`+`store_id`+`product_id` | periodic stock snapshots |
| `calendar` | `date` | daily calendar with seasonality/holiday/religious flags |

Column contracts live in code (`src/mri/synthetic/schema.py`, `ENTITY_COLUMNS`)
and are copied into every manifest, so downstream ingestion can rely on them.

Key column groups:

- **Money (MAD):** `unit_cost_mad`, `unit_price_mad`, `net_amount_mad`,
  `cost_amount_mad`, `margin_mad`, `total_cost_mad`, `stock_value_mad`.
- **Calendar flags:** `is_weekend`, `is_holiday`, `holiday_name`, `is_ramadan`,
  `is_eid_al_fitr`, `is_eid_al_adha`, `season`.
- **Nullable columns:** `sales.customer_id` (anonymous), `sales.promotion_id`,
  `promotions.region_id`, `promotions.channel_id`, `calendar.holiday_name`.

---

## 5. Relationships

```
regions 1───n stores
regions 1───n customers
regions 1───n promotions (optional scope)
channels 1──n customers (preferred_channel)
channels 1──n promotions (optional scope)

stores  1───n sales ──── n──1 products
stores  1───n sales ──── n──1 channels
stores  1───n sales ──── n──1 customers (nullable)
stores  1───n sales ──── n──1 promotions (nullable)

stores  1───n purchases ──n──1 products
stores  1───n inventory ──n──1 products
calendar 1──n sales (by sale_date)
```

Referential integrity, primary-key uniqueness and "sale never precedes customer
signup / store opening" are all enforced by tests.

---

## 6. Generation rules

### 6.1 Reference dimensions

- **Regions:** the 12 post-2015 Moroccan regions with simplified, normalised
  population/purchasing-power weights (`morocco.REGIONS`).
- **Channels:** in-store 0.78, e-commerce 0.14, mobile app 0.08 (normalised).
- **Calendar:** full daily coverage 2021-01-01 → 2024-12-31 (1461 days) with
  weekday, ISO week, quarter, season, national holidays, Islamic observances
  (fixed approximate dates for 2021–2024) and Ramadan flag.

### 6.2 Stores

Drawn from regional weights; format ∈ {hypermarket, supermarket, convenience,
cash & carry} with format-specific surface ranges. A per-store traffic factor
(format × lognormal noise × regional share) drives demand, so stores differ
substantially. ~4% are inactive; inactive stores receive no sales, purchases or
inventory. All stores open before the analysis period (2015-01-01 → 2020-12-31).

### 6.3 Products

Assigned across 7 categories / 28 subcategories with category-specific cost
ranges and gross-margin ranges (fresh/dairy low margin, beauty/household high).
`unit_price_mad` is derived from cost and margin, so `margin_pct = (price −
cost) / price` is always consistent and positive. Product **popularity** is a
Zipf-shaped rank (exponent 0.85) scaled by category turnover, producing a
realistic concentration (top ~10% of products ≈ 70%+ of sales). All products
launch before the analysis period.

### 6.4 Customers

Regional allocation by demand weight; signup dates 2017-01-01 → 2024-06-30;
age/segment/gender distributions; loyalty tier (bronze…platinum) multiplies a
lognormal purchase-frequency weight, giving a heavy-tailed buying frequency.
~2% are dormant (never purchase). Each customer has a preferred channel used
~60% of the time, producing observable channel affinity.

### 6.5 Promotions

Product-targeted with optional region and/or channel scope (70% national, 80%
all-channel). Types and discounts: `remise` 10–30%, `prix_promo` 15–25%, `bogo`
40%, `fidelite` 5–15%, `bundle` 18–28%. Windows are 7–21 days, preferentially
scheduled in high-demand periods. A promotion is applied to an eligible sale line
with 65% exposure probability; applied lines receive the promotion's discount
**capped at 90% of the product margin** (so `margin_mad` can never go negative)
and a +1-quantity lift with 40% probability.

### 6.6 Sales

Transaction-level generation expanded to line items:

1. Sale dates sampled from day weights combining weekday (Friday/Saturday
   highest), month, holiday (×1.30), Ramadan (×1.15), Eid al-Fitr (×1.60) and
   Eid al-Adha (×1.35) factors.
2. Baskets sized so each chunk yields **exactly** the requested number of sale
   lines; transaction attributes (date, store, hour, channel, customer) are
   consistent within a basket.
3. Opening hours 08:00–23:00, with separate weekday / weekend / Ramadan hour
   distributions (Ramadan shifts volume to the evening).
4. Customers sampled from the store's region ~70% of the time and from the
   national pool otherwise; only customers signed up on/before the sale date are
   eligible; ~10% of transactions are anonymous.
5. Products sampled from the **store's assortment** (a popularity-weighted
   subset of the catalogue, `assortment_ratio` of it) with popularity weights.
6. Quantity ∈ 1…5 (weighted), plus promotion lift.
7. Amounts: `net_amount_mad = round(qty × price × (1 − discount), 2)`,
   `cost_amount_mad = round(qty × unit_cost, 2)`,
   `margin_mad = max(0, net − cost)`.

### 6.7 Purchases and inventory

For every (store, product) pair in an assortment, demand is aggregated into
fixed windows (`snapshot_days` = 14 or 30 days). At each window start the store
replenishes up to `ceil(2 × max demand over the trailing 6 windows)`; purchases
are recorded with a small unit-cost jitter (σ = 1.2%). At each window end an
inventory snapshot is recorded with `stock_value_mad = qty × unit_cost` and
`stock_status ∈ {in_stock, low, out_of_stock}` (low when below 25% of target).
Dead windows (no stock, no demand, no receipt) are omitted.

---

## 7. Scale profiles

Counts are **documented targets**; the generator always reports the *actual*
row counts it produced.

| Profile | Target sales | Products | Stores | Customers | Promotions | Assortment | Snapshot |
|---------|-------------:|---------:|-------:|----------:|-----------:|-----------:|---------:|
| tiny | 2,000 | 60 | 4 | 200 | 20 | 0.50 | 30 d |
| small | 60,000 | 200 | 10 | 3,000 | 150 | 0.50 | 30 d |
| medium | 600,000 | 600 | 24 | 25,000 | 800 | 0.40 | 14 d |
| large | 3,000,000 | 1,200 | 40 | 120,000 | 3,000 | 0.40 | 14 d |
| xl | 8,000,000 | 2,500 | 60 | 400,000 | 8,000 | 0.35 | 14 d |

Reference runtimes (Windows 11, i5-8365U, SSD) are indicative only:
tiny ≈ 1 s, small ≈ 2 s, medium ≈ 25 s, large ≈ 100 s, xl ≈ 5.5 min.

---

## 8. Data-quality test fixtures

The generator produces **valid baseline data by default; nothing is corrupted**.
A separate, optional mechanism prepares controlled bad-data cases for future
Great Expectations testing:

```bash
uv run python -m apps.data_generator --dq-fixture null_required --seed 42
```

Scenarios (each a corrupted copy of the clean `tiny` baseline):

| Scenario | Corruption |
|----------|------------|
| `null_required` | nulls in required keys (e.g. `sales.store_id`, `products.category`) |
| `out_of_range` | negative/absurd quantities, prices, margins, birth years |
| `bad_dates` | invalid and out-of-period dates |
| `duplicate_keys` | duplicated `sale_id` / `store_id` rows |
| `schema_drift` | dropped / renamed / extra columns |

Fixtures are written to `data/dq_fixtures/<scenario>/` and **never** mixed with
`data/generated/`.

---

## 9. Limitations

- Synthetic only — distributions are plausible and internally consistent, not
  calibrated to a real retailer; no statistical-equivalence claim is made.
- Islamic observance dates (Ramadan, Aïd, Mawlid) are fixed Gregorian
  approximations for 2021–2024; no `holidays` runtime dependency is used.
- Products all launch before 2021; in-period product launches are not simulated.
- Prices/costs are static per product (no inflation or price-promo history beyond
  promotions); purchase unit cost only jitters ±~1–3%.
- Customers have no true behavioural state machine; frequency is a weighted
  random process (heavy-tailed but memoryless across purchases).
- Assortment is fixed per store for the whole period.
- No returns, refunds, cancellations, stock transfers or supplier dimension.
- CSV floats use Python `repr`; byte-level determinism is guaranteed on a fixed
  platform/version (verified for CSV). Parquet is supported for convenience and
  round-trips, but byte-level determinism is only asserted for CSV.
- `large`/`xl` exceed typical laptop RAM comfort for full in-memory downstream
  steps; use `medium` or smaller for interactive development.

---

## 10. Tests

`tests/unit/test_synthetic_*.py` cover: deterministic seed behaviour, required
columns, primary-key uniqueness, referential relationships, numeric ranges,
amount consistency, date ranges, signup/opening ordering, transaction
consistency, promotion application + scoping, and the writer/CLI/DQ-fixture
paths. Run:

```bash
uv run pytest tests/unit -q
```