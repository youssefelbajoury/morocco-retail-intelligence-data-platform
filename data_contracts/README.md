# Data Contracts

> **Status: POLICY ONLY (Phase 01).** The contract *framework* is defined here; the
> individual contracts are written in later phases, once ingestion is implemented.

A **data contract** is a versioned, machine-checkable agreement between a data
**producer** (API, file drop, synthetic generator) and its **consumers** (ingestion,
staging, dbt models). Nothing may enter the platform without a contract.

---

## 1. What a source contract contains

| Section | Content |
|---------|---------|
| `id` | Stable identifier, e.g. `sales_daily` |
| `version` | Semantic version of the contract itself, e.g. `1.0.0` |
| `owner` | Who produces/maintains the data |
| `source` | Endpoint, file pattern or generator entry point |
| `format` | `json`, `csv`, `parquet` + encoding/delimiter |
| `schema` | Field name → type, nullability, default/unit |
| `keys` | Primary/business keys of the entity |
| `freshness` | Expected update frequency and max acceptable staleness |
| `volume` | Expected row-count range per period |
| `quality` | Baseline rules: allowed values, ranges, referential expectations |
| `sla` | What consumers may rely on |

Contracts live in `data_contracts/` as one file per source (format decided in the
phase that implements ingestion — most likely YAML, validated by Python).

---

## 2. Versioning

- Contracts are **semantic-versioned** (`MAJOR.MINOR.PATCH`) and stored in Git.
- **PATCH** — documentation/metadata only (typo, owner change).
- **MINOR** — backward-compatible additions (new *optional* field, new metadata).
- **MAJOR** — breaking change (removed/renamed field, type change, key change).
- Every contract version is **immutable once released**; changes create a new version.

---

## 3. Schema evolution

| Change | Class | Handling |
|--------|-------|----------|
| New optional field | Compatible | MINOR bump; consumers ignore unknown fields |
| New required field with default | Compatible | MINOR bump; ingestion applies documented default |
| Field removed / renamed | Breaking | MAJOR bump; migration note + downstream impact review |
| Type widening (int → bigint) | Compatible | MINOR bump, documented |
| Type narrowing / semantics change | Breaking | MAJOR bump |
| Unit / currency change (e.g. MAD cents → MAD) | Breaking | MAJOR bump, always explicit |

---

## 4. Compatibility rules

- The platform supports **contract N and N-1** during a transition window.
- Ingestion refuses data whose payload **does not satisfy the declared contract**
  (fail fast, never "best effort" loading).
- Deprecated fields are marked `deprecated: true` before removal in a MAJOR bump.
- The synthetic generator and the external APIs obey the *same* contracts — one
  contract per logical entity, regardless of provenance.

---

## 5. Validation

Validation happens at three moments (all PLANNED):

1. **At ingestion** — payload checked against the contract (structure, types, keys).
2. **At the quality gate** — Great Expectations suites encode the contract's
   `quality` rules before data is trusted into Silver.
3. **In CI** — contract files themselves are linted/validated (schema of the schema)
   so a malformed contract cannot be merged.

Failures are **loud**: rejected records are counted and quarantined, never dropped
silently (see architectural principle *no silent data loss*).

---

## 6. Planned contracts (later phases)

| Contract id | Source | Notes |
|-------------|--------|-------|
| `sales_daily` | synthetic generator | core fact stream |
| `customers` | synthetic generator | PII-free simulated data |
| `products` | synthetic + Open Food Facts mapping | internal product dimension |
| `stores` / `regions` | synthetic | Moroccan regions |
| `inventory_daily` | synthetic | stock snapshots |
| `purchases` | synthetic | supplier purchases |
| `promotions` | synthetic | promotion calendar |
| `weather_daily` | Open-Meteo API | cached responses |
| `exchange_rates` | Frankfurter API | EUR/MAD, USD/MAD, GBP/MAD |
| `product_catalog_ext` | Open Food Facts | enrichment only |
