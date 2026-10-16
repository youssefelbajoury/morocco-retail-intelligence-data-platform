"""Seed Superset (Phase 12) — database, analytics datasets, charts, dashboards.

Run:  uv run python dashboards/superset/seed.py <step>
Steps:
  db         create the read-only warehouse connection (Retail Warehouse)
  datasets   create/refresh the four virtual analytics datasets
  charts     create/refresh all chart slices (idempotent by slice name)
  dashboards create/refresh the five dashboards + native filter config

All steps are idempotent — re-running never duplicates objects.
"""

import json
import os
import secrets
import sys
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("SUPERSET_URL", "http://localhost:8088")
ENV = {}
with open(os.path.join(os.path.dirname(__file__), "..", "..", ".env"), encoding="utf-8") as fh:
    for line in fh:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            ENV[k.strip()] = v.strip()

# Superset enforces CSRF on every mutation even with Bearer auth: fetch the
# token once (which also seeds the session cookie) and replay both.
_opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor())
_token = None
_csrf = None


def api(method, path, body=None, token=None):
    req = urllib.request.Request(BASE + path, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    if method != "GET" and _csrf:
        req.add_header("X-CSRFToken", _csrf)
    data = json.dumps(body).encode() if body is not None else None
    try:
        with _opener.open(req, data, timeout=60) as resp:
            payload = json.loads(resp.read() or b"{}")
            return resp.status, payload
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode(errors="replace")
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            pass
        return exc.code, payload


def login():
    global _token, _csrf
    status, out = api(
        "POST",
        "/api/v1/security/login",
        {
            "username": ENV.get("SUPERSET_ADMIN_USER", "admin"),
            "password": ENV["SUPERSET_ADMIN_PASSWORD"],
            "provider": "db",
            "refresh": False,
        },
    )
    if status != 200:
        sys.exit(f"login failed ({status}): {out}")
    _token = out["access_token"]
    status, out = api("GET", "/api/v1/security/csrf_token/", token=_token)
    if status != 200 or not out.get("result"):
        sys.exit(f"csrf token fetch failed ({status}): {out}")
    _csrf = out["result"]
    return _token


SALES_SQL = """
SELECT
  v.sale_id,
  v.transaction_id,
  d.date                  AS sale_date,
  d.year,
  d.quarter,
  d.month,
  d.iso_week,
  d.weekday_name,
  d.is_weekend,
  d.is_ramadan,
  d.season,
  v.hour_of_day,
  m.store_id,
  m.store_name,
  m.city,
  m.store_format,
  m.size_sqm,
  r.region_id,
  r.region_name,
  c.channel_name,
  c.channel_type,
  cl.customer_id,
  cl.segment,
  cl.loyalty_tier,
  cl.gender,
  cl.birth_year,
  p.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  p.unit_price_mad        AS list_price_mad,
  p.margin_pct            AS product_margin_pct,
  pr.promotion_id,
  pr.promotion_type,
  pr.discount_pct         AS promotion_discount_pct,
  v.quantity,
  v.unit_price_mad,
  v.discount_pct,
  v.net_amount_mad,
  v.cost_amount_mad,
  v.margin_mad
FROM warehouse.fact_ventes   v
JOIN warehouse.dim_date      d  ON d.date_sk    = v.date_sk
JOIN warehouse.dim_magasin   m  ON m.magasin_sk = v.magasin_sk
JOIN warehouse.dim_region    r  ON r.region_sk  = m.region_sk
JOIN warehouse.dim_canal     c  ON c.canal_sk   = v.canal_sk
JOIN warehouse.dim_client    cl ON cl.client_sk = v.client_sk
JOIN warehouse.dim_produit   p  ON p.produit_sk = v.produit_sk
LEFT JOIN warehouse.dim_promotion pr ON pr.promotion_sk = v.promotion_sk
""".strip()

PURCHASES_SQL = """
SELECT
  a.purchase_id,
  d.date                  AS purchase_date,
  d.year,
  d.quarter,
  d.month,
  d.iso_week,
  d.weekday_name,
  d.season,
  m.store_id,
  m.store_name,
  m.city,
  m.store_format,
  r.region_name,
  p.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  a.quantity,
  a.unit_cost_mad,
  a.total_cost_mad
FROM warehouse.fact_achats  a
JOIN warehouse.dim_date     d ON d.date_sk    = a.date_sk
JOIN warehouse.dim_magasin  m ON m.magasin_sk = a.magasin_sk
JOIN warehouse.dim_region   r ON r.region_sk  = m.region_sk
JOIN warehouse.dim_produit  p ON p.produit_sk = a.produit_sk
""".strip()

INVENTORY_SQL = """
SELECT
  s.stock_status,
  s.quantity_on_hand,
  s.stock_value_mad,
  d.date                  AS snapshot_date,
  d.year,
  d.quarter,
  d.month,
  d.iso_week,
  d.season,
  m.store_id,
  m.store_name,
  m.city,
  m.store_format,
  r.region_name,
  p.product_id,
  p.product_name,
  p.category,
  p.subcategory,
  p.brand,
  p.unit_cost_mad
FROM warehouse.fact_stock  s
JOIN warehouse.dim_date    d ON d.date_sk    = s.date_sk
JOIN warehouse.dim_magasin m ON m.magasin_sk = s.magasin_sk
JOIN warehouse.dim_region  r ON r.region_sk  = m.region_sk
JOIN warehouse.dim_produit p ON p.produit_sk = s.produit_sk
""".strip()

PLATFORM_SQL = """
SELECT
  load_id,
  run_id,
  batch_id,
  source,
  target_table,
  started_at,
  finished_at,
  EXTRACT(EPOCH FROM (finished_at - started_at)) AS duration_seconds,
  status,
  rows_read,
  rows_inserted,
  rows_rejected,
  error_message
FROM audit.load_audit
""".strip()

DATASETS = [
    ("sales_analytics", SALES_SQL),
    ("inventory_analytics", INVENTORY_SQL),
    ("purchases_analytics", PURCHASES_SQL),
    ("platform_health", PLATFORM_SQL),
]


def step_db(token):
    uri = (
        "postgresql+psycopg2://"
        + ENV["BI_READER_USER"]
        + ":"
        + ENV["BI_READER_PASSWORD"]
        + "@postgres:5432/retail"
    )
    body = {
        "database_name": "Retail Warehouse",
        "sqlalchemy_uri": uri,
        "expose_in_sqllab": True,
        "allow_cvas": True,
        "allow_dml": False,
        "allow_run_async": False,
    }
    status, out = api("GET", "/api/v1/database/?q=(page_size:1000)", token=token)
    rows = [d for d in out.get("result", []) if d["database_name"] == "Retail Warehouse"]
    if rows:
        print(f"database already exists id={rows[0]['id']}")
        db_id = rows[0]["id"]
    else:
        status, out = api("POST", "/api/v1/database/", body, token)
        if status in (200, 201):
            print(f"database created id={out.get('id')}")
            db_id = out.get("id")
        else:
            print(f"database create -> {status}: {out}")
            sys.exit("database create failed")
    status, test = api(
        "POST",
        "/api/v1/database/test_connection/",
        {"database_name": "Retail Warehouse", "sqlalchemy_uri": uri},
        token,
    )
    print(f"test_connection -> {status}: {test}")
    return db_id


def step_datasets(token, db_id):
    status, me = api("GET", "/api/v1/me/", token=token)
    owner = me["result"]["id"]
    status, out = api("GET", "/api/v1/dataset/?q=(page_size:1000)", token=token)
    existing = {d["table_name"]: d["id"] for d in out.get("result", [])}
    for name, sql in DATASETS:
        if name in existing:
            ds_id = existing[name]
            body = {"sql": sql, "owners": [owner]}
            status, out = api("PUT", f"/api/v1/dataset/{ds_id}", body, token)
            print(f"dataset updated {name} id={ds_id} -> {status}")
        else:
            body = {
                "database": db_id,
                "table_name": name,
                "sql": sql,
                "owners": [owner],
            }
            status, out = api("POST", "/api/v1/dataset/", body, token)
            created = out if status not in (200, 201) else out.get("id")
            print(f"dataset created {name} -> {status}: {created}")
        if status not in (200, 201):
            print("   ", out)
    status, out = api("GET", "/api/v1/dataset/?q=(page_size:1000)", token=token)
    for d in out.get("result", []):
        print(f"   id={d['id']} {d['table_name']}")


def metric(expr, label):
    """Ad-hoc SQL metric object for chart form_data."""
    return {
        "expressionType": "SQL",
        "sqlExpression": expr,
        "label": label,
        "optionName": "m_" + label,
    }


def make_kpi(name, expr, label, subheader, fmt="SMART_NUMBER"):
    return {
        "name": name,
        "params": {
            "viz_type": "big_number_total",
            "metric": metric(expr, label),
            "subheader": subheader,
            "y_axis_format": fmt,
        },
        "height": 26,
    }


def make_series(name, time_col, metrics, grain="P1M", viz="echarts_timeseries_line"):
    return {
        "name": name,
        "params": {
            "viz_type": viz,
            "granularity_sqla": time_col,
            "time_range": "No filter",
            "time_grain_sqla": grain,
            "metrics": metrics,
            "groupby": [],
            "row_limit": 10000,
        },
        "height": 48,
    }


def make_pie(name, time_col, groupby, metrics):
    return {
        "name": name,
        "params": {
            "viz_type": "pie",
            "granularity_sqla": time_col,
            "time_range": "No filter",
            "groupby": [groupby],
            "metric": metrics[0],
            "metrics": metrics,
            "row_limit": 1000,
        },
        "height": 48,
    }


def make_table(name, time_col, groupby, metrics, limit=100):
    return {
        "name": name,
        "params": {
            "viz_type": "table",
            "granularity_sqla": time_col,
            "time_range": "No filter",
            "groupby": list(groupby),
            "metrics": list(metrics),
            "row_limit": limit,
            "order_desc": True,
            "table_timestamp_format": "smart_date",
        },
        "height": 50,
    }


NET = metric("SUM(net_amount_mad)", "net_revenue")
MARGIN = metric("SUM(margin_mad)", "margin")
COST = metric("SUM(total_cost_mad)", "cost")
QTY = metric("SUM(quantity)", "quantity")

DASHBOARDS = [
    {
        "title": "Sales Performance",
        "dataset": "sales_analytics",
        "time_col": "sale_date",
        "charts": {
            "net_revenue": make_kpi("Net Revenue", "SUM(net_amount_mad)", "net_revenue", "MAD"),
            "gross_margin": make_kpi("Gross Margin", "SUM(margin_mad)", "gross_margin", "MAD"),
            "tickets": make_kpi(
                "Tickets Sold", "COUNT(DISTINCT transaction_id)", "tickets", "tickets"
            ),
            "units_sold": make_kpi("Units Sold", "SUM(quantity)", "units_sold", "units"),
            "rev_margin_trend": make_series(
                "Revenue & Margin Trend",
                "sale_date",
                [NET, MARGIN],
            ),
            "channel_share": make_pie(
                "Revenue by Channel",
                "sale_date",
                "channel_name",
                [NET],
            ),
            "monthly_rev": make_series(
                "Monthly Revenue",
                "sale_date",
                [NET],
                viz="echarts_timeseries_bar",
            ),
            "region_share": make_pie(
                "Revenue by Region",
                "sale_date",
                "region_name",
                [NET],
            ),
            "top_products": make_table(
                "Top 10 Products by Revenue",
                "sale_date",
                ["product_name"],
                [NET],
                limit=10,
            ),
        },
        "rows": [
            [("net_revenue", 3), ("gross_margin", 3), ("tickets", 3), ("units_sold", 3)],
            [("rev_margin_trend", 8), ("channel_share", 4)],
            [("monthly_rev", 8), ("region_share", 4)],
            [("top_products", 12)],
        ],
        "filters": [
            ("time", "Time Range"),
            ("select", "Region", "region_name"),
            ("select", "Channel", "channel_name"),
            ("select", "Category", "category"),
        ],
    },
    {
        "title": "Executive Overview",
        "dataset": "sales_analytics",
        "time_col": "sale_date",
        "charts": {
            "exec_revenue": make_kpi("Revenue", "SUM(net_amount_mad)", "net_revenue", "MAD"),
            "margin_rate": make_kpi(
                "Margin Rate",
                "SUM(margin_mad) / NULLIF(SUM(net_amount_mad), 0) * 100",
                "margin_rate",
                "%",
                fmt=",.1f",
            ),
            "avg_basket": make_kpi(
                "Average Basket",
                "SUM(net_amount_mad) / NULLIF(COUNT(DISTINCT transaction_id), 0)",
                "avg_basket",
                "MAD / ticket",
                fmt=",.0f",
            ),
            "active_stores": make_kpi(
                "Active Stores",
                "COUNT(DISTINCT store_id)",
                "active_stores",
                "stores",
            ),
            "revenue_trend": make_series(
                "Monthly Revenue Trend",
                "sale_date",
                [NET],
            ),
            "category_share": make_pie(
                "Revenue by Category",
                "sale_date",
                "category",
                [NET],
            ),
            "region_margin": make_pie(
                "Margin by Region",
                "sale_date",
                "region_name",
                [MARGIN],
            ),
            "top_products_margin": make_table(
                "Top 10 Products by Margin",
                "sale_date",
                ["product_name"],
                [MARGIN],
                limit=10,
            ),
        },
        "rows": [
            [("exec_revenue", 3), ("margin_rate", 3), ("avg_basket", 3), ("active_stores", 3)],
            [("revenue_trend", 8), ("category_share", 4)],
            [("region_margin", 6), ("top_products_margin", 6)],
        ],
        "filters": [
            ("time", "Time Range"),
            ("select", "Region", "region_name"),
            ("select", "Category", "category"),
        ],
    },
    {
        "title": "Inventory Health",
        "dataset": "inventory_analytics",
        "time_col": "snapshot_date",
        "charts": {
            "stock_value": make_kpi("Stock Value", "SUM(stock_value_mad)", "stock_value", "MAD"),
            "units_on_hand": make_kpi(
                "Units on Hand", "SUM(quantity_on_hand)", "units_on_hand", "units"
            ),
            "low_stock": make_kpi(
                "Low-stock SKUs",
                "COUNT(DISTINCT CASE WHEN stock_status = 'low' THEN product_id END)",
                "low_stock",
                "SKUs",
            ),
            "out_of_stock": make_kpi(
                "Out-of-stock SKUs",
                "COUNT(DISTINCT CASE WHEN stock_status = 'out_of_stock' THEN product_id END)",
                "out_of_stock",
                "SKUs",
            ),
            "stock_trend": make_series(
                "Stock Value Trend",
                "snapshot_date",
                [metric("SUM(stock_value_mad)", "stock_value")],
            ),
            "status_share": make_pie(
                "Stock by Status",
                "snapshot_date",
                "stock_status",
                [metric("SUM(stock_value_mad)", "status_value")],
            ),
            "region_stock": make_pie(
                "Regional Stock Value",
                "snapshot_date",
                "region_name",
                [metric("SUM(stock_value_mad)", "region_stock")],
            ),
            "store_category_stock": make_table(
                "Stock by Store & Category",
                "snapshot_date",
                ["store_name", "category"],
                [metric("SUM(stock_value_mad)", "stock_value")],
                limit=100,
            ),
        },
        "rows": [
            [("stock_value", 3), ("units_on_hand", 3), ("low_stock", 3), ("out_of_stock", 3)],
            [("stock_trend", 8), ("status_share", 4)],
            [("region_stock", 4), ("store_category_stock", 8)],
        ],
        "filters": [
            ("time", "Time Range"),
            ("select", "Region", "region_name"),
            ("select", "Stock Status", "stock_status"),
        ],
    },
    {
        "title": "Procurement",
        "dataset": "purchases_analytics",
        "time_col": "purchase_date",
        "charts": {
            "purchase_cost": make_kpi("Purchase Cost", "SUM(total_cost_mad)", "cost", "MAD"),
            "units_purchased": make_kpi("Units Purchased", "SUM(quantity)", "quantity", "units"),
            "purchase_lines": make_kpi(
                "Purchase Lines",
                "COUNT(DISTINCT purchase_id)",
                "purchase_lines",
                "lines",
            ),
            "avg_unit_cost": make_kpi(
                "Average Unit Cost",
                "SUM(total_cost_mad) / NULLIF(SUM(quantity), 0)",
                "avg_unit_cost",
                "MAD",
                fmt=",.2f",
            ),
            "cost_trend": make_series(
                "Purchase Cost Trend",
                "purchase_date",
                [COST],
            ),
            "cost_category": make_pie(
                "Cost by Category",
                "purchase_date",
                "category",
                [COST],
            ),
            "cost_region": make_pie(
                "Regional Purchase Cost",
                "purchase_date",
                "region_name",
                [COST],
            ),
            "top_stores": make_table(
                "Top Stores by Purchase Cost",
                "purchase_date",
                ["store_name"],
                [COST],
                limit=15,
            ),
        },
        "rows": [
            [
                ("purchase_cost", 3),
                ("units_purchased", 3),
                ("purchase_lines", 3),
                ("avg_unit_cost", 3),
            ],
            [("cost_trend", 8), ("cost_category", 4)],
            [("cost_region", 4), ("top_stores", 8)],
        ],
        "filters": [
            ("time", "Time Range"),
            ("select", "Region", "region_name"),
            ("select", "Category", "category"),
        ],
    },
    {
        "title": "Platform Health",
        "dataset": "platform_health",
        "time_col": "started_at",
        "charts": {
            "pipeline_loads": make_kpi("Pipeline Loads", "COUNT(*)", "loads", "loads"),
            "rows_inserted": make_kpi(
                "Rows Inserted", "SUM(rows_inserted)", "rows_inserted", "rows"
            ),
            "rows_rejected": make_kpi(
                "Rows Rejected", "SUM(rows_rejected)", "rows_rejected", "rows"
            ),
            "skipped_loads": make_kpi(
                "Skipped Loads",
                "COUNT(CASE WHEN status = 'skipped' THEN 1 END)",
                "skipped_loads",
                "loads",
            ),
            "duration_trend": make_series(
                "Load Duration Trend",
                "started_at",
                [metric("AVG(duration_seconds)", "avg_duration")],
                grain="P1D",
            ),
            "loads_source": make_pie(
                "Loads by Source",
                "started_at",
                "source",
                [metric("COUNT(*)", "loads")],
            ),
            "rows_trend": make_series(
                "Rows Processed Trend",
                "started_at",
                [
                    metric("SUM(rows_read)", "rows_read"),
                    metric("SUM(rows_inserted)", "rows_inserted"),
                ],
                grain="P1D",
            ),
            "loads_status": make_pie(
                "Loads by Status",
                "started_at",
                "status",
                [metric("COUNT(*)", "loads")],
            ),
            "load_audit": make_table(
                "Load Audit Detail",
                "started_at",
                ["source", "target_table", "status"],
                [
                    metric("COUNT(*)", "loads"),
                    metric("SUM(rows_inserted)", "rows_inserted"),
                    metric("SUM(rows_rejected)", "rows_rejected"),
                ],
                limit=100,
            ),
        },
        "rows": [
            [
                ("pipeline_loads", 3),
                ("rows_inserted", 3),
                ("rows_rejected", 3),
                ("skipped_loads", 3),
            ],
            [("duration_trend", 8), ("loads_source", 4)],
            [("rows_trend", 8), ("loads_status", 4)],
            [("load_audit", 12)],
        ],
        "filters": [
            ("time", "Time Range"),
            ("select", "Source", "source"),
        ],
    },
]


def _fetch_datasets(token):
    status, out = api("GET", "/api/v1/dataset/?q=(page_size:1000)", token=token)
    if status != 200:
        sys.exit(f"dataset list -> {status}: {out}")
    res = {}
    for d in out.get("result", []):
        res[d["table_name"]] = {"id": d["id"], "uuid": d.get("uuid")}
    for name, info in res.items():
        if not info["uuid"]:
            status, det = api("GET", f"/api/v1/dataset/{info['id']}", token=token)
            res[name]["uuid"] = det.get("result", {}).get("uuid")
    return res


def _resolve_charts(token, owner, ds):
    status, out = api("GET", "/api/v1/chart/?q=(page_size:1000)", token=token)
    existing = {c["slice_name"]: c["id"] for c in out.get("result", [])}
    charts = {}
    seen = set()
    for dash in DASHBOARDS:
        dsinfo = ds[dash["dataset"]]
        for key, spec in dash["charts"].items():
            name = spec["name"]
            if name in seen:
                sys.exit(f"duplicate slice name: {name}")
            seen.add(name)
            cid = existing.get(name)
            body = {
                "datasource_id": dsinfo["id"],
                "datasource_type": "table",
                "slice_name": name,
                "viz_type": spec["params"]["viz_type"],
                "params": json.dumps(spec["params"]),
                "owners": [owner],
            }
            if cid is None:
                body["uuid"] = str(uuid.uuid4())
                status, out = api("POST", "/api/v1/chart/", body, token)
                if status not in (200, 201):
                    sys.exit(f"chart create {name} -> {status}: {out}")
                cid = out["id"]
                print(f"chart created  {name} id={cid}")
            else:
                status, out = api("PUT", f"/api/v1/chart/{cid}", body, token)
                if status != 200:
                    sys.exit(f"chart update {name} -> {status}: {out}")
                print(f"chart exists   {name} id={cid}")
            status, det = api("GET", f"/api/v1/chart/{cid}", token=token)
            charts[(dash["title"], key)] = {
                "id": cid,
                "uuid": det["result"]["uuid"],
                "name": name,
            }
    return charts


def _build_filter(dash, dsinfo, spec, charts):
    fid = "NATIVE_FILTER-" + secrets.token_urlsafe(16)
    ids = [c["id"] for c in charts.values()]
    kind = spec[0]
    common = {
        "cascadeParentIds": [],
        "chartsInScope": ids,
        "defaultDataMask": {"extraFormData": {}, "filterState": {}, "ownState": {}},
        "description": "",
        "id": fid,
        "scope": {"excluded": [], "rootPath": ["ROOT_ID"]},
        "tabsInScope": [],
        "type": "NATIVE_FILTER",
    }
    if kind == "time":
        return {
            **common,
            "name": spec[1],
            "controlValues": {"enableEmptyFilter": False},
            "filterType": "filter_time",
            "targets": [{}],
        }
    return {
        **common,
        "name": spec[1],
        "controlValues": {
            "defaultToFirstItem": False,
            "enableEmptyFilter": False,
            "inverseSelection": False,
            "multiSelect": True,
            "searchAllOptions": False,
        },
        "filterType": "filter_select",
        "targets": [
            {
                "column": {"name": spec[2]},
                "datasetUuid": dsinfo["uuid"],
                "datasetId": dsinfo["id"],
            }
        ],
    }


def _build_layout(dash, charts):
    grid_children = []
    pos = {
        "HEADER_ID": {"id": "HEADER_ID", "meta": {"text": dash["title"]}, "type": "HEADER"},
        "ROOT_ID": {"id": "ROOT_ID", "children": ["GRID_ID"], "type": "ROOT"},
        "GRID_ID": {
            "id": "GRID_ID",
            "children": [],
            "parents": ["ROOT_ID"],
            "type": "GRID",
        },
        "DASHBOARD_VERSION_KEY": "v2",
    }
    for row in dash["rows"]:
        row_id = "ROW-" + secrets.token_urlsafe(8)
        grid_children.append(row_id)
        row_children = []
        for key, width in row:
            cinfo = charts[(dash["title"], key)]
            cid = "CHART-" + secrets.token_urlsafe(8)
            pos[cid] = {
                "children": [],
                "id": cid,
                "meta": {
                    "chartId": cinfo["id"],
                    "height": dash["charts"][key]["height"],
                    "sliceName": cinfo["name"],
                    "uuid": cinfo["uuid"],
                    "width": width,
                },
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "type": "CHART",
            }
            row_children.append(cid)
        pos[row_id] = {
            "children": row_children,
            "id": row_id,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
            "parents": ["ROOT_ID", "GRID_ID"],
            "type": "ROW",
        }
    pos["GRID_ID"]["children"] = grid_children
    return pos


def _build_metadata(dash, dsinfo, charts):
    ids = [c["id"] for c in charts.values()]
    chart_configuration = {
        c["uuid"]: {
            "id": c["uuid"],
            "crossFilters": {"chartsInScope": ids, "scope": "global"},
        }
        for c in charts.values()
    }
    native_filters = [_build_filter(dash, dsinfo, spec, charts) for spec in dash["filters"]]
    return {
        "chart_configuration": chart_configuration,
        "color_scheme": "supersetColors",
        "cross_filters_enabled": True,
        "default_filters": "{}",
        "expanded_slices": {},
        "global_chart_configuration": {
            "chartsInScope": ids,
            "scope": {"excluded": [], "rootPath": ["ROOT_ID"]},
        },
        "native_filter_configuration": native_filters,
        "refresh_frequency": 0,
        "timed_refresh_immune_slices": [],
    }


def _create_dashboards(token, owner, ds, charts=None):
    if charts is None:
        charts = _resolve_charts(token, owner, ds)
    status, out = api("GET", "/api/v1/dashboard/?q=(page_size:1000)", token=token)
    existing = {d["dashboard_title"]: d["id"] for d in out.get("result", [])}
    for dash in DASHBOARDS:
        dsinfo = ds[dash["dataset"]]
        position = _build_layout(dash, charts)
        metadata = _build_metadata(dash, dsinfo, charts)
        body = {
            "dashboard_title": dash["title"],
            "position_json": json.dumps(position),
            "json_metadata": json.dumps(metadata),
            "published": True,
        }
        dash_id = existing.get(dash["title"])
        if dash_id is None:
            body["uuid"] = str(uuid.uuid4())
            status, out = api("POST", "/api/v1/dashboard/", body, token)
            if status not in (200, 201):
                sys.exit(f"dashboard create {dash['title']} -> {status}: {out}")
            dash_id = out["id"]
            print(f"dashboard created {dash['title']} id={dash_id}")
        else:
            status, out = api("PUT", f"/api/v1/dashboard/{dash_id}", body, token)
            if status != 200:
                sys.exit(f"dashboard update {dash['title']} -> {status}: {out}")
            print(f"dashboard updated {dash['title']} id={dash_id}")
        for (title, _key), cinfo in charts.items():
            if title != dash["title"]:
                continue
            status, out = api(
                "PUT",
                f"/api/v1/chart/{cinfo['id']}",
                {"dashboards": [dash_id]},
                token,
            )
            if status != 200:
                print(f"   warn: linking chart {cinfo['name']} -> {status}: {out}")


def main():
    step = sys.argv[1] if len(sys.argv) > 1 else "all"
    token = login()
    status, me = api("GET", "/api/v1/me/", token=token)
    owner = me["result"]["id"]
    db_id = None
    if step in ("db", "all"):
        db_id = step_db(token)
    elif step in ("datasets", "charts", "dashboards"):
        status, out = api("GET", "/api/v1/database/?page_size=100", token=token)
        db_id = next(d["id"] for d in out["result"] if d["database_name"] == "Retail Warehouse")
    if step in ("datasets", "all"):
        step_datasets(token, db_id)
    ds = None
    charts = None
    if step in ("charts", "dashboards", "all"):
        ds = _fetch_datasets(token)
    if step in ("charts", "all"):
        charts = _resolve_charts(token, owner, ds)
    if step in ("dashboards", "all"):
        _create_dashboards(token, owner, ds, charts)


if __name__ == "__main__":
    main()
