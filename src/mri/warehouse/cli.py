"""Command-line interface for the star-schema warehouse.

Usage examples::

    python -m apps.warehouse plan
    python -m apps.warehouse --json plan
    python -m apps.warehouse build
    python -m apps.warehouse --json build
    python -m apps.warehouse --schema warehouse_it build
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

from mri.ingestion.config import Settings
from mri.ingestion.exceptions import IngestionError, UsageError
from mri.ingestion.manifest import new_run_id
from mri.observability import runtime as observability
from mri.warehouse import (
    WarehouseBuilder,
    WarehouseBuildResult,
    render_load_statements,
    render_truncate_statement,
    render_warehouse_ddl,
)

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apps.warehouse",
        description="Rebuild the Kimball star-schema warehouse from the Silver layer.",
    )
    parser.add_argument(
        "--json", action="store_true", dest="as_json", help="emit machine-readable JSON"
    )
    parser.add_argument(
        "--dag-run-id",
        default="",
        help="Airflow DAG run identity to correlate this stage with (traceability)",
    )
    parser.add_argument(
        "--observability-dir",
        type=Path,
        default=None,
        help="structured JSONL event log directory (default: settings)",
    )
    parser.add_argument(
        "--schema", metavar="NAME", help="warehouse schema (default: settings/warehouse)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "build",
        help="truncate, reload and reconcile every warehouse table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  apps.warehouse build\n"
            "  apps.warehouse --json build\n"
            "  apps.warehouse --schema warehouse_it build\n"
            "flow: silver -> TRUNCATE ... RESTART IDENTITY -> 7 dimensions -> "
            "3 facts -> reconciliation (row counts + measure sums)\n"
            "exit codes: 0 ok, 1 build failed or reconciliation unbalanced, "
            "2 usage error"
        ),
    )
    sub.add_parser(
        "plan",
        help="print DDL, truncate and load statements without connecting",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  apps.warehouse plan\n"
            "  apps.warehouse --json plan --schema analytics\n"
            "exit codes: 0 ok, 2 usage error"
        ),
    )
    return parser


def _settings(args: argparse.Namespace) -> Settings:
    settings = Settings.from_env(observability_dir=getattr(args, "observability_dir", None))
    if not args.schema:
        return settings
    if not IDENTIFIER_PATTERN.match(args.schema or ""):
        raise UsageError(f"--schema must be a plain SQL identifier, got {args.schema!r}")
    return replace(settings, warehouse_schema=args.schema)


def _cmd_plan(args: argparse.Namespace) -> int:
    settings = _settings(args)
    warehouse_schema = settings.warehouse_schema
    statements = [
        *render_warehouse_ddl(
            warehouse_schema=warehouse_schema, silver_schema=settings.silver_schema
        ),
        render_truncate_statement(warehouse_schema=warehouse_schema),
        *[
            sql
            for _, sql in render_load_statements(
                warehouse_schema=warehouse_schema, silver_schema=settings.silver_schema
            )
        ],
    ]
    if args.as_json:
        print(
            json.dumps(
                {"schema": warehouse_schema, "statements": statements},
                indent=2,
                ensure_ascii=False,
            )
        )
    else:
        print(f"warehouse plan for schema {warehouse_schema} ({len(statements)} statements)")
        for statement in statements:
            print(statement)
    return EXIT_OK


def _summarise(result: WarehouseBuildResult) -> None:
    print(
        f"warehouse run {result.run_id}: schema={result.schemas['warehouse']} "
        f"silver={result.schemas['silver']} tables={len(result.tables)}"
    )
    for entry in result.reconciliation:
        status = "OK" if entry.balanced else "UNBALANCED"
        measures = " ".join(
            f"{measure.metric}={measure.silver}/{measure.warehouse}" for measure in entry.measures
        )
        detail = f" {measures}" if measures else ""
        print(
            f"  [{status}] {entry.target_table} <- {entry.source_table}: "
            f"silver={entry.silver_rows} warehouse={entry.warehouse_rows}{detail}"
        )
    timing = result.timing
    print(
        f"timing: ddl={timing.get('ddl_seconds', 0.0)}s "
        f"load={timing.get('load_seconds', 0.0)}s "
        f"reconcile={timing.get('reconcile_seconds', 0.0)}s "
        f"total={timing.get('total_seconds', 0.0)}s"
    )


def _cmd_build(args: argparse.Namespace) -> int:
    settings = _settings(args)
    run_id = new_run_id()
    started = time.perf_counter()
    observability.emit(
        pipeline="warehouse",
        stage="warehouse",
        run_id=run_id,
        status="started",
        message=f"warehouse build {run_id} starting",
        payload={
            "schema": settings.warehouse_schema,
            "silver_schema": settings.silver_schema,
        },
    )
    result = WarehouseBuilder(settings).build(run_id=run_id)
    for entry in result.reconciliation:
        ok = entry.balanced
        observability.emit(
            pipeline="warehouse",
            stage="warehouse",
            run_id=result.run_id,
            status=("success" if ok else "failed"),
            source=entry.source_table,
            batch_id=entry.target_table,
            records_read=entry.silver_rows,
            records_written=entry.warehouse_rows,
            error_code=("" if ok else "WAREHOUSE_UNBALANCED"),
            message=(
                f"{entry.target_table} <- {entry.source_table}: "
                f"silver={entry.silver_rows} warehouse={entry.warehouse_rows}"
            ),
        )
    ok = result.ok
    observability.emit(
        pipeline="warehouse",
        stage="warehouse",
        run_id=result.run_id,
        status=("success" if ok else "failed"),
        records_written=sum(result.tables.values()),
        duration_ms=observability.elapsed_ms(started),
        error_code=("" if ok else "WAREHOUSE_FAILED"),
        message=("warehouse build finished: OK" if ok else "warehouse build finished: FAILED"),
        payload={
            "tables": dict(result.tables),
            "timing": dict(result.timing),
            "schema": settings.warehouse_schema,
        },
    )
    if args.as_json:
        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
    else:
        _summarise(result)
    return EXIT_OK if result.ok else EXIT_FAILURE


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    observability.set_context(
        dag_run_id=args.dag_run_id or "",
        events_dir=args.observability_dir or Settings.from_env().observability_dir,
    )
    handlers = {
        "build": _cmd_build,
        "plan": _cmd_plan,
    }
    try:
        return handlers[args.command](args)
    except UsageError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except IngestionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_FAILURE


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
