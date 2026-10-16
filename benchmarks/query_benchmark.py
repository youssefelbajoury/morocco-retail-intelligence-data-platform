"""Query-performance benchmark: five representative business queries.

The queries run read-only against a populated warehouse star schema and are
measured with ``EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)``:

1. revenue by month and region (time + geography)
2. margin by product category
3. inventory valuation by product category
4. purchase trend by month
5. top products by revenue

Planner statistics are refreshed once with ``ANALYZE`` (non-destructive), each
query is executed a warm-up plus ``--repeats`` measured runs, and the median
execution time plus the last plan (planning/execution time, shared hit/read
blocks, actual rows) is appended to ``benchmarks/queries/results.json``.

Usage (from the repository root):

    uv run python benchmarks/query_benchmark.py --scale small
    uv run python benchmarks/query_benchmark.py --scale medium --repeats 3
    uv run python benchmarks/query_benchmark.py --schema warehouse --allow-dev
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from mri.ingestion.config import Settings  # noqa: E402
from mri.ingestion.exceptions import IngestionError  # noqa: E402
from mri.silver import conninfo_for  # noqa: E402
from mri.warehouse.schema import TABLES  # noqa: E402

RESULTS_PATH = Path(__file__).resolve().parent / "queries" / "results.json"
SCALES = ("small", "medium", "1m", "5m")

QUERIES: tuple[tuple[str, str], ...] = (
    (
        "revenue_by_month_region",
        """
        SELECT d.year, d.month, r.region_name,
               sum(f.net_amount_mad) AS revenue,
               count(*) AS lines
        FROM {schema}.fact_ventes AS f
        JOIN {schema}.dim_date AS d ON d.date_sk = f.date_sk
        JOIN {schema}.dim_magasin AS m ON m.magasin_sk = f.magasin_sk
        JOIN {schema}.dim_region AS r ON r.region_sk = m.region_sk
        GROUP BY d.year, d.month, r.region_name
        ORDER BY d.year, d.month, revenue DESC
        """,
    ),
    (
        "margin_by_category",
        """
        SELECT p.category,
               sum(f.net_amount_mad) AS revenue,
               sum(f.margin_mad) AS margin,
               sum(f.margin_mad) / nullif(sum(f.net_amount_mad), 0) AS margin_pct
        FROM {schema}.fact_ventes AS f
        JOIN {schema}.dim_produit AS p ON p.produit_sk = f.produit_sk
        GROUP BY p.category
        ORDER BY margin DESC
        """,
    ),
    (
        "inventory_value_by_category",
        """
        SELECT p.category,
               sum(s.stock_value_mad) AS stock_value,
               sum(s.quantity_on_hand) AS units
        FROM {schema}.fact_stock AS s
        JOIN {schema}.dim_produit AS p ON p.produit_sk = s.produit_sk
        GROUP BY p.category
        ORDER BY stock_value DESC
        """,
    ),
    (
        "purchase_trend_by_month",
        """
        SELECT d.year, d.month,
               sum(a.total_cost_mad) AS purchases,
               sum(a.quantity) AS units
        FROM {schema}.fact_achats AS a
        JOIN {schema}.dim_date AS d ON d.date_sk = a.date_sk
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month
        """,
    ),
    (
        "top_products_by_revenue",
        """
        SELECT p.product_id, p.product_name, sum(f.net_amount_mad) AS revenue
        FROM {schema}.fact_ventes AS f
        JOIN {schema}.dim_produit AS p ON p.produit_sk = f.produit_sk
        GROUP BY p.product_id, p.product_name
        ORDER BY revenue DESC
        LIMIT 20
        """,
    ),
)


def _analyze(settings: Settings, schema: str) -> None:
    with psycopg.connect(conninfo_for(settings)) as connection:
        for table in TABLES:
            connection.execute(f'ANALYZE "{schema}"."{table.name}"')


def _explain(connection: psycopg.Connection, sql: str) -> dict[str, Any]:
    with connection.cursor() as cur:
        cur.execute(f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {sql}")
        (plan,) = cur.fetchone()
    node = plan[0]
    inner = node.get("Plan", {})
    hit = int(inner.get("Shared Hit Blocks", 0))
    read = int(inner.get("Shared Read Blocks", 0))
    return {
        "planning_time_ms": round(float(node.get("Planning Time", 0.0)), 3),
        "execution_time_ms": round(float(node.get("Execution Time", 0.0)), 3),
        "actual_rows": int(inner.get("Actual Rows", 0)),
        "shared_hit_blocks": hit,
        "shared_read_blocks": read,
        "plan": inner,
    }


def _row_count(connection: psycopg.Connection, sql: str) -> int:
    with connection.cursor() as cur:
        cur.execute(sql)
        return len(cur.fetchall())


def run(schema: str, *, repeats: int) -> dict[str, Any]:
    settings = Settings.from_env()
    print(f"analyzing {schema} ...")
    _analyze(settings, schema)

    measures: list[dict[str, Any]] = []
    with psycopg.connect(conninfo_for(settings)) as connection:
        for name, template in QUERIES:
            sql = template.format(schema=schema)
            rows = _row_count(connection, sql)
            _explain(connection, sql)  # warm-up
            samples = [_explain(connection, sql) for _ in range(repeats)]
            exec_times = [sample["execution_time_ms"] for sample in samples]
            measures.append(
                {
                    "name": name,
                    "result_rows": rows,
                    "median_execution_ms": round(statistics.median(exec_times), 3),
                    "min_execution_ms": min(exec_times),
                    "max_execution_ms": max(exec_times),
                    "last": samples[-1],
                }
            )
            print(f"  {name}: rows={rows} median={statistics.median(exec_times):.1f}ms")

    entry: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "schema": schema,
        "repeats": repeats,
        "queries": measures,
    }
    return entry


def _append(entry: dict[str, Any]) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    history: list[dict[str, Any]] = []
    if RESULTS_PATH.is_file():
        history = json.loads(RESULTS_PATH.read_text(encoding="utf-8"))
    history.append(entry)
    RESULTS_PATH.write_text(
        json.dumps(history, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark representative queries")
    parser.add_argument("--scale", choices=SCALES, default="small")
    parser.add_argument("--schema", default="", help="override the warehouse schema")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--allow-dev", action="store_true", help="allow the dev schema")
    parser.add_argument("--no-record", action="store_true")
    args = parser.parse_args(argv)

    schema = args.schema or f"warehouse_e2e_{args.scale}"
    if not args.allow_dev and not schema.endswith(f"_e2e_{args.scale}") and args.scale != "dev":
        if schema == "warehouse":
            print("refusing to query the dev schema without --allow-dev", file=sys.stderr)
            return 2

    try:
        entry = run(schema, repeats=max(1, args.repeats))
    except (IngestionError, psycopg.Error) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if not args.no_record:
        _append(entry)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
