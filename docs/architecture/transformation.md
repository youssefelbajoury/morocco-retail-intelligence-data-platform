# Transformation & Warehouse Architecture

> **Status: PLANNED.** No dbt project, model, test or warehouse table exists yet.

---

## 1. Transformation architecture (dbt)

dbt owns **all analytics-shaped SQL** after data is trusted in PostgreSQL staging.
Python never writes business logic that dbt should own; dbt never fetches external data.

```mermaid
flowchart TB
    STG["staging schema (SILVER)<br/>1:1 cleaned source tables"]
    INT["intermediate models<br/>conformed, deduplicated, enriched"]
    MRT["marts models<br/>business entities per domain"]
    DWH["warehouse tables<br/>facts + dimensions (star schema)"]
    T1["dbt tests<br/>uniqueness, not-null, relationships,<br/>accepted values, custom tests"]
    DOC["dbt docs / lineage"]

    STG --> INT --> MRT --> DWH
    T1 -. asserts .- INT
    T1 -. asserts .- MRT
    T1 -. asserts .- DWH
    DOC -. generated from .- MRT
```

### Planned dbt project layout

```
dbt/
├── dbt_project.yml            # project + model configuration
├── profiles.yml               # connection (env-driven, never committed secrets)
├── models/
│   ├── staging/               # 1:1 with sources, thin cleanup only
│   ├── intermediate/          # reusable conformed logic
│   └── marts/                 # business marts (sales, inventory, customer, ...)
├── seeds/                     # small static CSV (calendar, regions, holidays)
├── macros/                    # shared SQL helpers
├── tests/                     # singular (data) tests
└── snapshots/                 # slowly changing dimension captures
```

### Transformation rules (designed)

| Rule | Detail |
|------|--------|
| Layer discipline | Staging never joins other entities; marts never re-clean raw strings. |
| Naming | `stg_<entity>`, `int_<verb>_<entity>`, `<domain>_<grain>` (fct_/dim_). |
| Tests are mandatory | Every mart ships with not-null/unique/relationship tests. |
| Incremental where it pays | Large fact models use incremental materialization keyed on a load timestamp. |
| Deterministic | No `now()`-dependent logic in model definitions; as-of dates passed as variables. |

---

## 2. Warehouse architecture (PostgreSQL star schema)

The final serving model is a **dimensional star schema** in PostgreSQL, consumed by
Superset.

```mermaid
flowchart LR
    subgraph Dimensions["Dimensions (PLANNED)"]
        D1["dim_date<br/>(calendars, holidays, Ramadan/Aid flags)"]
        D2["dim_customer"]
        D3["dim_product"]
        D4["dim_store"]
        D5["dim_region<br/>(Moroccan regions)"]
        D6["dim_channel<br/>(store / e-commerce)"]
        D7["dim_promotion"]
        D8["dim_weather"]
        D9["dim_exchange_rate"]
    end

    subgraph Facts["Facts (PLANNED)"]
        F1["fact_sales"]
        F2["fact_inventory"]
        F3["fact_purchases"]
    end

    F1 --- D1
    F1 --- D2
    F1 --- D3
    F1 --- D4
    F1 --- D6
    F1 --- D7
    F1 --- D8
    F2 --- D1
    F2 --- D3
    F2 --- D4
    F3 --- D1
    F3 --- D3
    F3 --- D5
```

| Decision | Rationale |
|----------|-----------|
| Star schema (not snowflake) | BI-friendly, matches Superset's implicit join model, simpler for dashboards. |
| Conformed dimensions | `dim_date`, `dim_product`, `dim_store` shared across all marts. |
| Postgres as warehouse | Single relational engine for staging + serving keeps the local stack small; swap-ready for a bigger engine later. |
| Surrogate keys | Integer keys generated in dbt; source natural keys kept as attributes. |
| Schema management | `warehouse/migrations/` holds ordered SQL migrations; `warehouse/schemas/` documents the target DDL. |

### Separation of duties

```
Python  → moves & types data        (ingestion)
GE      → certifies data quality    (gate)
dbt     → models & tests            (analytics)
Postgres→ stores & serves           (system of record for BI)
Superset→ visualizes                (consumption)
```
