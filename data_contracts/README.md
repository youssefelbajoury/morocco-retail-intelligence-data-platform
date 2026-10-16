# Data Contracts

> **Status: POLICY + IMPLEMENTED.** The framework below was written in Phase 01;
> Phase 04 implemented it: one **YAML** contract per source now exists in this
> directory (`<source>.yml`, all at `version: 1.0.0`), enforced by
> `src/mri/ingestion/contracts.py`. Phase 07 added four **external** contracts
> (`open_meteo_history`, `open_meteo_forecast`, `frankfurter_fx`,
> `openfoodfacts_products`) for the verbatim API responses — 14 in total, with
> one documented deviation from the one-entity rule (see §4 and
> [`ADR-005`](../docs/decisions/ADR-005-external-bronze-contracts.md)).
> Phase 15 added the **contract drift detector**
> (`src/mri/governance/drift.py`), which proves in CI that every declared rule
> is implemented and vice-versa (see §5.3 and
> [`ADR-012`](../docs/decisions/ADR-012-contract-drift-detection.md)).
> Rendered reference:
> [`docs/data-model/source-contracts.md`](../docs/data-model/source-contracts.md).

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
| `format` | `csv`, `json` (+ encoding/delimiter) |
| `columns` | Field name → type, required/nullable, description |
| `keys` | Primary/business keys of the entity |
| `freshness` | Expected update frequency and max acceptable staleness |
| `volume` | Expected row-count range per period |
| `quality` | Baseline rules: allowed values, ranges, referential expectations |
| `sla` | What consumers may rely on |

Contracts live in `data_contracts/` as **one YAML file per source**
(`<source>.yml`). The format was fixed in Phase 04 (see ADR-002).

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
  contract per logical entity, regardless of provenance — **with one documented
  exception (Phase 07 / [`ADR-005`](../docs/decisions/ADR-005-external-bronze-contracts.md))**:
  the four `raw_external/` sources get their *own* contracts describing the
  fetch result (request-shaped payloads with provenance context), because a
  stored FX response or a single Open Food Facts barcode fetch does not have
  the grain or keys of the entity it later enriches.

---

## 5. Validation

Validation happens at three moments:

1. **At ingestion — IMPLEMENTED (Phase 04).** The payload is checked against the
   contract (format, required columns, null-safety, basic types) and rejected
   files are quarantined under `rejected/contracts/` with a `.reason.json`
   sidecar. See [`docs/pipeline/file-ingestion.md`](../docs/pipeline/file-ingestion.md).
2. **At the quality gate — IMPLEMENTED (Phase 06, Bronze).** Great Expectations
   suites encode the contract's volume, type, nullability and uniqueness rules
   plus its `quality.declared` rules (ranges, sets, pair comparisons) and
   cross-source referential checks; a batch is only promoted on a full pass.
   See [`docs/pipeline/data-quality.md`](../docs/pipeline/data-quality.md).
3. **In CI — IMPLEMENTED (Phase 15).** Every contract file is parsed and
   self-validated by the loader as part of the test-suite
   (`tests/unit/test_ingestion.py`), so a malformed contract fails the build.
   On top of that, the **contract drift detector**
   (`src/mri/governance/drift.py`, `tests/unit/test_contract_drift.py`) proves
   that the runtime enforces exactly what the contracts declare: one canonical
   rule model is produced from both the `quality.declared` text and the
   expectations the bronze gate generates, and a rule on either side without a
   counterpart fails the build. Source mappings, contract versions, reserved
   provenance column names and Bronze partition columns are checked the same
   way. See
   [`docs/data-model/contract-governance.md`](../docs/data-model/contract-governance.md)
   and [`ADR-012`](../docs/decisions/ADR-012-contract-drift-detection.md).

Failures are **loud**: rejected records are counted and quarantined, never dropped
silently (see architectural principle *no silent data loss*).

---

## 6. Contract inventory

| Contract id | Status | Source | Notes |
|-------------|--------|--------|-------|
| `regions` | **v1.0.0 (Phase 04)** | synthetic generator | Moroccan regions |
| `channels` | **v1.0.0 (Phase 04)** | synthetic generator | physical / digital channels |
| `stores` | **v1.0.0 (Phase 04)** | synthetic generator | store dimension |
| `products` | **v1.0.0 (Phase 04)** | synthetic generator | internal product dimension |
| `customers` | **v1.0.0 (Phase 04)** | synthetic generator | PII-free simulated data |
| `promotions` | **v1.0.0 (Phase 04)** | synthetic generator | promotion calendar |
| `sales` | **v1.0.0 (Phase 04)** | synthetic generator | core fact stream |
| `purchases` | **v1.0.0 (Phase 04)** | synthetic generator | supplier purchases |
| `inventory` | **v1.0.0 (Phase 04)** | synthetic generator | stock snapshots |
| `calendar` | **v1.0.0 (Phase 04)** | synthetic generator | weekdays, holidays, Ramadan |
| `open_meteo_history` | **v1.0.0 (Phase 07)** | Open-Meteo archive API | daily weather observations per region |
| `open_meteo_forecast` | **v1.0.0 (Phase 07)** | Open-Meteo forecast API | daily forecast per region |
| `frankfurter_fx` | **v1.0.0 (Phase 07)** | Frankfurter API | EUR/MAD, EUR/USD, EUR/GBP daily rates |
| `openfoodfacts_products` | **v1.0.0 (Phase 07)** | Open Food Facts | one enriched product per fetched barcode |

Product enrichment stays under `openfoodfacts_products`; it never overwrites the
internal `products` contract — `mapping_status` / `external_product_id` join
them downstream (see [`ADR-005`](../docs/decisions/ADR-005-external-bronze-contracts.md)).
