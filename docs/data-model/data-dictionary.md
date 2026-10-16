# Data Dictionary

This is the **entry point** to the platform's data dictionary. It explains the
layers and points to the authoritative, per-table references. Machine-readable
definitions live in code (`TABLES` in `src/mri/warehouse/schema.py`) and in the
`data_contracts/*.yml` files; the documents below render them.

> **All data is synthetic** (seeded generator) plus public, keyless API
> enrichment. No real personal or proprietary data is present.

## Layers and where they are defined

| Layer | Store | Schema / prefix | Definition |
|-------|-------|-----------------|------------|
| RAW | MinIO | `raw/`, `raw_external/` | [`../pipeline/file-ingestion.md`](../pipeline/file-ingestion.md), [`../pipeline/external-apis.md`](../pipeline/external-apis.md) |
| BRONZE | MinIO | `bronze/` | [`../pipeline/bronze-layer.md`](../pipeline/bronze-layer.md) |
| SILVER | PostgreSQL | `raw_staging`, `silver`, `audit` | [`../pipeline/silver-layer.md`](../pipeline/silver-layer.md) |
| INTERMEDIATE / MARTS | dbt | `dbt_staging`, `dbt_intermediate`, `dbt_marts` | [`dbt.md`](dbt.md) |
| WAREHOUSE (star schema) | PostgreSQL | `warehouse` | [`star-schema.md`](star-schema.md) · [`facts-and-dimensions.md`](facts-and-dimensions.md) |
| SCD type 2 | dbt | `dbt_snapshots.dim_produit` | [`scd2.md`](scd2.md) |

## Source contracts

Every source (14 in total, v1.0.0) has a versioned contract describing columns,
types, keys and guarantees:

- Policy: [`data_contracts/README.md`](../../data_contracts/README.md)
- Rendered reference: [`source-contracts.md`](source-contracts.md)

Contracts are the single source of truth and are checked against the runtime code
by the drift detector (see [`contract-governance.md`](contract-governance.md)).

## Star-schema tables (business dictionary)

- **Dimensions (7):** `dim_date`, `dim_client`, `dim_produit`, `dim_magasin`,
  `dim_region`, `dim_canal`, `dim_promotion`.
- **Facts (3):** `fact_ventes` (sale line), `fact_achats` (receipt line),
  `fact_stock` (daily date/store/product snapshot).

Per-table **grain, keys and column-by-column reference**:
[`facts-and-dimensions.md`](facts-and-dimensions.md).

## Collation / encoding

- PostgreSQL database `retail` (UTF-8). Money columns are MAD (`*_mad`).
- Dates use ISO-8601; `dim_date` derives `year`, `month`, `iso_week`, `season`,
  `is_ramadan`, and related calendar attributes.
