# Source Contracts

> **Status: IMPLEMENTED (Phases 04 + 07).** One YAML contract per source lives in
> `data_contracts/<source>.yml` — fourteen in total: ten entity-shaped
> synthetic sources (Phase 04) and four **external** contracts for the
> verbatim API responses (`open_meteo_history`, `open_meteo_forecast`,
> `frankfurter_fx`, `openfoodfacts_products`, Phase 07). This page is the
> rendered reference of those contracts — the machine-readable source of
> truth remains the YAML files.

The contract policy (meaning of `version`, compatibility and the schema
evolution rules) is defined in
[`data_contracts/README.md`](../../data_contracts/README.md). The design
rationale for the format and for content-addressed raw storage is recorded in
[`ADR-002`](../decisions/ADR-002-source-contracts-and-raw-ingestion.md).

---

## 1. Anatomy of a contract

```yaml
id: sales
version: 1.0.0            # semantic, MAJOR is checked by the loader
owner: synthetic-generator
source: "mri.synthetic generator (apps.data_generator)"
format:
  types: [csv, json]      # only these formats are accepted
  encoding: utf-8
  delimiter: ","
keys:
  primary: [sale_id]      # composite keys are supported
columns:
  - {name: sale_id, type: string, required: true, nullable: false, description: "..."}
freshness: {frequency: daily, max_age: null}
volume: {min_rows: 1, max_rows: null}
quality:
  declared:               # documented, executed later by Great Expectations
    - "sale_id is unique and not null"
sla:
  consumers: [bronze, staging]
```

Supported types: `string`, `integer`, `number`, `boolean`, `date` (ISO-8601
`YYYY-MM-DD`).

| Field | Enforced in Phase 04 |
|-------|----------------------|
| `format.types`, `encoding`, `delimiter` | yes — `FORMAT_NOT_ALLOWED` |
| `columns[].required` | yes — `MISSING_COLUMN` |
| `columns[].nullable` | yes — `MISSING_VALUE` |
| `columns[].type` | yes — `TYPE_MISMATCH` (sample of 1000 rows) |
| `keys.primary` | presence only; uniqueness is a Great Expectations rule |
| `quality.declared` | no — declared documentation |

---

## 2. Versioning

All contracts currently declare **`1.0.0`**. Bump rules:

- **PATCH** — wording/owner/description only.
- **MINOR** — backward-compatible addition (new optional column, new metadata).
- **MAJOR** — removed/renamed column, type change, key change.

Consumers can pin a version with `python -m apps.ingestion validate
--expect-version 1.0.0`.

---

## 3. Contract reference### `regions` (v1.0.0)

Moroccan regions with normalised demand weights.

Primary key: `region_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `region_id` | `string` | yes | no | Region code, e.g. R01 |
| `region_name` | `string` | yes | no | Official Moroccan region name |
| `demand_weight` | `number` | yes | no | Normalised demand weight in [0, 1] |

### `channels` (v1.0.0)

Sales channels (physical stores, e-commerce, mobile app).

Primary key: `channel_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `channel_id` | `string` | yes | no | Channel code, e.g. CH01 |
| `channel_name` | `string` | yes | no | Human-readable channel name |
| `channel_type` | `string` | yes | no | One of: physical, digital |
| `demand_weight` | `number` | yes | no | Normalised demand weight in [0, 1] |

### `stores` (v1.0.0)

Retail stores, each belonging to one Moroccan region.

Primary key: `store_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `store_id` | `string` | yes | no | Store code, e.g. ST001 |
| `store_name` | `string` | yes | no | Store name |
| `city` | `string` | yes | no | City name |
| `region_id` | `string` | yes | no | FK -> regions.region_id |
| `store_format` | `string` | yes | no | One of: hypermarket, supermarket, convenience, cash_and_carry |
| `size_sqm` | `integer` | yes | no | Selling surface in square metres |
| `open_date` | `date` | yes | no | Opening date (ISO-8601 yyyy-mm-dd) |
| `is_active` | `boolean` | yes | no | Active-store flag |

### `products` (v1.0.0)

Product catalogue with cost, price and margin.

Primary key: `product_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `product_id` | `string` | yes | no | Product code, e.g. P000001 |
| `product_name` | `string` | yes | no | Product display name |
| `category` | `string` | yes | no | Top-level category |
| `subcategory` | `string` | yes | no | Subcategory |
| `brand` | `string` | yes | no | Brand name |
| `unit_cost_mad` | `number` | yes | no | Unit cost in MAD (> 0) |
| `unit_price_mad` | `number` | yes | no | Unit price in MAD (>= cost) |
| `margin_pct` | `number` | yes | no | Gross margin ratio in [0, 1) |
| `launch_date` | `date` | yes | no | Launch date (ISO-8601 yyyy-mm-dd) |
| `is_active` | `boolean` | yes | no | Active-product flag |

### `customers` (v1.0.0)

Simulated customers with region, segment and loyalty attributes.

Primary key: `customer_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `customer_id` | `string` | yes | no | Customer code, e.g. CU000001 |
| `signup_date` | `date` | yes | no | Signup date (ISO-8601 yyyy-mm-dd) |
| `region_id` | `string` | yes | no | FK -> regions.region_id |
| `birth_year` | `integer` | yes | no | Year of birth |
| `gender` | `string` | yes | no | One of: F, M, U |
| `segment` | `string` | yes | no | One of: jeune, famille, senior, actif |
| `loyalty_tier` | `string` | yes | no | One of: bronze, silver, gold, platinum |
| `preferred_channel_id` | `string` | yes | no | FK -> channels.channel_id |
| `is_active` | `boolean` | yes | no | Active-customer flag |

### `promotions` (v1.0.0)

Product promotions with optional region/channel scope and a date window.

Primary key: `promotion_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `promotion_id` | `string` | yes | no | Promotion code, e.g. PM0001 |
| `product_id` | `string` | yes | no | FK -> products.product_id |
| `region_id` | `string` | no | yes | Optional FK -> regions.region_id (null = all regions) |
| `channel_id` | `string` | no | yes | Optional FK -> channels.channel_id (null = all channels) |
| `promotion_type` | `string` | yes | no | One of: remise, prix_promo, bogo, fidelite, bundle |
| `discount_pct` | `number` | yes | no | Discount ratio in [0, 1] |
| `start_date` | `date` | yes | no | Start date (ISO-8601 yyyy-mm-dd) |
| `end_date` | `date` | yes | no | End date (ISO-8601 yyyy-mm-dd) |

### `sales` (v1.0.0)

Sale line items (one row per product within a shopping transaction).

Primary key: `sale_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `sale_id` | `string` | yes | no | Sale line code, e.g. S000000000 |
| `transaction_id` | `string` | yes | no | Basket id grouping sale lines, e.g. T000000000 |
| `sale_date` | `date` | yes | no | Sale date (ISO-8601 yyyy-mm-dd) |
| `hour_of_day` | `integer` | yes | no | Hour of day in [8, 23] |
| `store_id` | `string` | yes | no | FK -> stores.store_id |
| `channel_id` | `string` | yes | no | FK -> channels.channel_id |
| `customer_id` | `string` | no | yes | Optional FK -> customers.customer_id (null = anonymous) |
| `product_id` | `string` | yes | no | FK -> products.product_id |
| `promotion_id` | `string` | no | yes | Optional FK -> promotions.promotion_id (null = no promotion) |
| `quantity` | `integer` | yes | no | Units sold (>= 1) |
| `unit_price_mad` | `number` | yes | no | Unit price in MAD at sale time |
| `discount_pct` | `number` | yes | no | Applied discount ratio in [0, 1] |
| `net_amount_mad` | `number` | yes | no | Net amount in MAD (>= 0) |
| `cost_amount_mad` | `number` | yes | no | Cost amount in MAD (>= 0) |
| `margin_mad` | `number` | yes | no | Margin in MAD (>= 0) |

### `purchases` (v1.0.0)

Store replenishment receipts (store receiving products).

Primary key: `purchase_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `purchase_id` | `string` | yes | no | Purchase code, e.g. PU00000001 |
| `purchase_date` | `date` | yes | no | Receipt date (ISO-8601 yyyy-mm-dd) |
| `store_id` | `string` | yes | no | FK -> stores.store_id |
| `product_id` | `string` | yes | no | FK -> products.product_id |
| `quantity` | `integer` | yes | no | Units received (>= 1) |
| `unit_cost_mad` | `number` | yes | no | Unit cost in MAD (> 0) |
| `total_cost_mad` | `number` | yes | no | Total cost in MAD (>= 0) |

### `inventory` (v1.0.0)

Periodic stock snapshots per store and product.

Primary key: `snapshot_date`, `store_id`, `product_id`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `snapshot_date` | `date` | yes | no | Snapshot date (ISO-8601 yyyy-mm-dd) |
| `store_id` | `string` | yes | no | FK -> stores.store_id |
| `product_id` | `string` | yes | no | FK -> products.product_id |
| `quantity_on_hand` | `integer` | yes | no | Units on hand (>= 0) |
| `stock_value_mad` | `number` | yes | no | Stock value in MAD (>= 0) |
| `stock_status` | `string` | yes | no | One of: in_stock, low, out_of_stock |

### `calendar` (v1.0.0)

Daily calendar with weekday, holiday and Islamic-observance flags.

Primary key: `date`
Format: csv, json

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `date` | `date` | yes | no | Calendar date (ISO-8601 yyyy-mm-dd) |
| `year` | `integer` | yes | no | Calendar year |
| `month` | `integer` | yes | no | Month 1-12 |
| `quarter` | `integer` | yes | no | Quarter 1-4 |
| `iso_week` | `integer` | yes | no | ISO week number |
| `weekday` | `integer` | yes | no | Weekday 0=Monday .. 6=Sunday |
| `weekday_name` | `string` | yes | no | English weekday name |
| `is_weekend` | `boolean` | yes | no | Weekend flag |
| `is_holiday` | `boolean` | yes | no | National-holiday flag |
| `holiday_name` | `string` | no | yes | Holiday name (null when not a holiday) |
| `is_ramadan` | `boolean` | yes | no | Ramadan flag |
| `is_eid_al_fitr` | `boolean` | yes | no | Eid al-Fitr flag |
| `is_eid_al_adha` | `boolean` | yes | no | Eid al-Adha flag |
| `season` | `string` | yes | no | One of: winter, spring, summer, autumn |

### `open_meteo_history` (v1.0.0)

*External contract (Phase 07).* Observed daily weather per project region —
one row per observed day, flattened from a verbatim `raw_external/` response
plus its metadata sidecar.

Primary key: `region_id`, `observation_date`
Format: json
Owner: external-api

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `region_id` | `string` | yes | no | Project region id from the request context, e.g. R01 |
| `region_name` | `string` | yes | no | Official Moroccan region name |
| `city` | `string` | yes | no | Chief city the coordinates belong to |
| `timezone` | `string` | yes | no | Requested timezone, e.g. Africa/Casablanca |
| `latitude` | `number` | yes | no | WGS84 latitude of the requested location |
| `longitude` | `number` | yes | no | WGS84 longitude of the requested location |
| `observation_date` | `date` | yes | no | Observed day (ISO-8601 yyyy-mm-dd) |
| `data_type` | `string` | yes | no | Payload data type: historical |
| `retrieval_time` | `string` | yes | no | metadata.retrieved_at of the raw response (ISO-8601) |
| `weather_code` | `integer` | yes | yes | WMO weather code (null when the API omits the value) |
| `temperature_2m_max` | `number` | yes | yes | Daily maximum 2m temperature in degrees Celsius |
| `temperature_2m_min` | `number` | yes | yes | Daily minimum 2m temperature in degrees Celsius |
| `precipitation_sum` | `number` | yes | yes | Daily precipitation sum in mm |
| `precipitation_hours` | `number` | yes | yes | Hours with precipitation in the day (0-24) |
| `wind_speed_10m_max` | `number` | yes | yes | Daily maximum 10m wind speed in km/h |

### `open_meteo_forecast` (v1.0.0)

*External contract (Phase 07).* Forward-looking daily weather per project
region — one row per forecast day; same shape as
`open_meteo_history` with `forecast_date` / `data_type: forecast`.

Primary key: `region_id`, `forecast_date`
Format: json
Owner: external-api

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `region_id` | `string` | yes | no | Project region id from the request context, e.g. R01 |
| `region_name` | `string` | yes | no | Official Moroccan region name |
| `city` | `string` | yes | no | Chief city the coordinates belong to |
| `timezone` | `string` | yes | no | Requested timezone, e.g. Africa/Casablanca |
| `latitude` | `number` | yes | no | WGS84 latitude of the requested location |
| `longitude` | `number` | yes | no | WGS84 longitude of the requested location |
| `forecast_date` | `date` | yes | no | Forecast day (ISO-8601 yyyy-mm-dd) |
| `data_type` | `string` | yes | no | Payload data type: forecast |
| `retrieval_time` | `string` | yes | no | metadata.retrieved_at of the raw response (ISO-8601) |
| `weather_code` | `integer` | yes | yes | WMO weather code (null when the API omits the value) |
| `temperature_2m_max` | `number` | yes | yes | Forecast maximum 2m temperature in degrees Celsius |
| `temperature_2m_min` | `number` | yes | yes | Forecast minimum 2m temperature in degrees Celsius |
| `precipitation_sum` | `number` | yes | yes | Forecast precipitation sum in mm |
| `precipitation_hours` | `number` | yes | yes | Forecast hours with precipitation (0-24) |
| `wind_speed_10m_max` | `number` | yes | yes | Forecast maximum 10m wind speed in km/h |

### `frankfurter_fx` (v1.0.0)

*External contract (Phase 07).* EUR reference exchange rates — one row per
(date, base, quote) rate from the verbatim Frankfurter response.

Primary key: `date`, `base`, `quote`
Format: json
Owner: external-api

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `date` | `date` | yes | no | Rate validity day (ISO-8601 yyyy-mm-dd) |
| `base` | `string` | yes | no | Base currency, always EUR |
| `quote` | `string` | yes | no | Quote currency: MAD, USD or GBP |
| `rate` | `number` | yes | no | ECB reference rate (base units per quote unit), > 0 |

### `openfoodfacts_products` (v1.0.0)

*External contract (Phase 07).* Product-catalog enrichment keyed by barcode —
one row per fetched barcode (found **or** not found). Enrichment only; it
never overwrites the internal `products` contract (ADR-002/ADR-005).

Primary key: `external_product_id`
Format: json
Owner: external-api

| Column | Type | Required | Nullable | Description |
|--------|------|----------|----------|-------------|
| `external_product_id` | `string` | yes | no | Exact barcode (8-14 digits) from the mapping sidecar or payload code |
| `internal_product_id` | `string` | yes | yes | Linked internal product_id on exact barcode match (null = NEW/UNMATCHED) |
| `mapping_status` | `string` | yes | no | MATCHED, NEW or UNMATCHED |
| `retrieval_time` | `string` | yes | no | metadata.retrieved_at of the raw response (ISO-8601) |
| `product_name` | `string` | yes | yes | Product name as published by Open Food Facts |
| `brands` | `string` | yes | yes | Brand list, comma separated |
| `quantity` | `string` | yes | yes | Declared quantity as text, e.g. '400 g e' |
| `categories` | `string` | yes | yes | Category labels, comma separated |
| `nutriscore_grade` | `string` | yes | yes | Nutri-Score grade a-e (null when not computed) |
| `nova_group` | `integer` | yes | yes | NOVA food-processing group 1-4 (null when unknown) |
| `image_url` | `string` | yes | yes | Front image URL (null when absent) |
| `countries` | `string` | yes | yes | Countries where the product is sold, comma separated |
| `lang` | `string` | yes | yes | Main language code of the entry, e.g. fr |
| `ingredients_text` | `string` | yes | yes | Ingredient list as text (may be empty) |
| `allergens` | `string` | yes | yes | Allergen labels, comma separated |
| `product_quantity` | `number` | yes | yes | Declared pack quantity in grams (null when absent) |
| `energy_kcal_100g` | `number` | yes | yes | Energy per 100 g in kcal (null when absent) |
| `fat_100g` | `number` | yes | yes | Fat per 100 g in grams (null when absent) |
| `sugars_100g` | `number` | yes | yes | Sugars per 100 g in grams (null when absent) |
| `proteins_100g` | `number` | yes | yes | Proteins per 100 g in grams (null when absent) |
| `salt_100g` | `number` | yes | yes | Salt per 100 g in grams (null when absent) |
