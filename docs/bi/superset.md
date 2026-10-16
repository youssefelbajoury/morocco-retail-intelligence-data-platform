# Apache Superset BI Layer (Phase 12)

Status: **IMPLEMENTED** — Apache Superset 6.1.0 reads the star-schema warehouse
through a dedicated read-only role and serves five interactive dashboards
(42 charts) with native filters.

- Decision: [`docs/decisions/ADR-010-superset-bi-layer.md`](../decisions/ADR-010-superset-bi-layer.md)
- Seeding tool: [`dashboards/superset/seed.py`](../../dashboards/superset/seed.py)
- Verification tool: [`dashboards/superset/verify.py`](../../dashboards/superset/verify.py)

---

## Topology

A deliberately light, single-user local deployment layered on the Phase 11 base
stack. Both Compose files are always used **in one project**:

```powershell
docker compose --env-file .env `
  -f infrastructure/docker/docker-compose.yml `
  -f infrastructure/docker/docker-compose.superset.yml up -d
```

| Service | Image | Host address | Purpose |
|---------|-------|--------------|---------|
| `superset-init` | `mri-superset:6.1.0` (one-shot) | — | metadata migrations + admin user + roles |
| `superset` | `mri-superset:6.1.0` | `localhost:8088` | web server, UI + REST API |

`mri-superset:6.1.0` extends the official `apache/superset:6.1.0` image by adding
`psycopg2-binary==2.9.10` (the stock image ships no PostgreSQL driver) —
see [`infrastructure/docker/Dockerfile.superset`](../../infrastructure/docker/Dockerfile.superset).

Deliberately **not** included: Redis, a PostgreSQL metadata database, Celery
workers/beat, and example dashboards. Superset's metadata lives in SQLite inside
the named volume `superset_home`. Everything runs on `mri-net` and reaches the
warehouse by **service name `postgres`**, never `localhost`. Credentials come
exclusively from the root `.env`.

Login: `http://localhost:8088` with `SUPERSET_ADMIN_USER` /
`SUPERSET_ADMIN_PASSWORD` from the repository `.env`.

---

## Database connection (read-only)

The Superset database object **Retail Warehouse** points at the retail
PostgreSQL (`POSTGRES_DB`, host `postgres`, port 5432) and logs in as the
read-only role `BI_READER_USER` / `BI_READER_PASSWORD`.

The role is provisioned by
[`infrastructure/sql/bi-reader-readonly.sql`](../../infrastructure/sql/bi-reader-readonly.sql)
(apply once as a privileged user):

```powershell
docker exec -i mri-postgres psql -U retail_app -d retail `
  -f infrastructure/sql/bi-reader-readonly.sql
```

Grants (all `SELECT`-only, plus `ALTER DEFAULT PRIVILEGES` for future objects):

| Schema | Access |
|--------|--------|
| `warehouse` | CONNECT + USAGE + SELECT |
| `audit` | CONNECT + USAGE + SELECT |
| `dbt_marts` | CONNECT + USAGE + SELECT |

Verified idempotent; re-running is harmless. Superset can never write to the
warehouse.

---

## Virtual datasets

Seeding creates four virtual datasets (SQL-defined views over the warehouse and
audit schemas) and one database:

| Dataset | Base tables | Drives dashboards |
|---------|-------------|-------------------|
| `sales_analytics` | `warehouse.fact_ventes` + `dim_date/magasin/region/canal/client/produit/promotion` | Sales Performance, Executive Overview |
| `inventory_analytics` | `warehouse.fact_stock` + `dim_date/magasin/region/produit` | Inventory Health |
| `purchases_analytics` | `warehouse.fact_achats` + `dim_date/magasin/region/produit` | Procurement |
| `platform_health` | `audit.load_audit` | Platform Health |

---

## Dashboards & charts

42 charts across 5 dashboards, all Query-Object (virtual-dataset) sources,
no embedded SQL in the chart layer beyond ad-hoc metrics:

| Dashboard | Charts | Native filters | Layout |
|-----------|--------|----------------|--------|
| Sales Performance | 9 | Time Range, Region, Channel, Category | 4 KPI row + line, pie, bar, pie, table |
| Executive Overview | 8 | Time Range, Region, Category | 4 KPI row + line/bar/pie/table |
| Inventory Health | 8 | Time Range, Region, Stock Status | 4 KPI row + trend/status pie/regional pie/table |
| Procurement | 8 | Time Range, Region, Category | 4 KPI row + cost trend/bars + table |
| Platform Health | 9 | Time Range, Source | 4 KPI row + duration/rows trend + source/status pies + audit table |

Chart types used: `big_number_total` (KPIs), `echarts_timeseries_line`,
`echarts_timeseries_bar`, `pie`, `table`. Every chart is bound to exactly one
dashboard, `position_json` uses **layout v2** (`HEADER → ROOT → GRID → ROW →
CHART`, `DASHBOARD_VERSION_KEY: "v2"`), and `json_metadata` carries
`native_filter_configuration` (+ `chart_configuration` / `cross_filters_enabled`
/ `global_chart_configuration`). Filter targets use `datasetUuid`, as required
by Superset 6.1 canonical exports.

Saved screenshots at [`docs/bi/screenshots/`](screenshots/) (headless Edge
render, 1600×1000, full page).

---

## Seeding & verification

The seed is API-driven (Bearer token + CSRF header) and idempotent — re-runs
update in place and never duplicate objects:

```powershell
uv run python dashboards/superset/seed.py all        # db + datasets + charts + dashboards
uv run python dashboards/superset/seed.py db         # one step at a time
uv run python dashboards/superset/seed.py datasets
uv run python dashboards/superset/seed.py charts
uv run python dashboards/superset/seed.py dashboards
uv run python dashboards/superset/verify.py          # assertions, exit != 0 on failure
```

`verify.py` checks: 4 datasets, 42 charts (unique names), 5 dashboards with the
expected number of chart slots, v2 layouts, no orphaned chart references,
native filters with `chartsInScope`, and executes all four virtual-dataset SQLs
through the sql-lab API (HTTP 200, no error payload).

### Known caveats

- `GET /api/v1/chart/{id}/data/` returns **"no query context saved"** for
  API-created charts: that endpoint only replays the cached query from a UI
  session. Rendering is verified headlessly instead (all 42 slots render with
  zero JS errors across the 5 dashboards).
- Filter *select controls* render in the header bar, but the headless
  automation used here does not drive the popover to select a value. Filter
  **wiring** is verified structurally (`chartsInScope`, filter types via
  `verify.py`) and the filter **data path** is verified (filtering narrows live
  results), but the **interactive in-browser filter application is UNVERIFIED**
  — no browser-automation tooling is present. See
  [`docs/reports/final-acceptance.md`](../reports/final-acceptance.md) §20 (U1).

---

## Idempotent replay (after `docker compose down -v`)

```powershell
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml `
  -f infrastructure/docker/docker-compose.superset.yml up -d
# wait for "healthy"
uv run python dashboards/superset/seed.py all
uv run python dashboards/superset/verify.py
```