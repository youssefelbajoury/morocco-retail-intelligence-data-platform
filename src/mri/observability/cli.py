"""Command-line interface for the operational audit model.

Usage examples::

    python -m apps.observability record --dag-run-id dag__123 --pipeline airflow \\
        --stage dag_run --status success
    python -m apps.observability metrics --json
    python -m apps.observability trace --dag-run-id dag__123
    python -m apps.observability trace --batch-id sales__0123456789abcdef
    python -m apps.observability sync

``record`` is the Airflow hook: the DAG's final task emits the DAG-run event.
``metrics`` and ``trace`` read the local JSONL event log (no database needed);
``sync`` bulk-loads it into the PostgreSQL ``ops`` schema so the metric views
can be queried. Credentials are never accepted or logged by any command.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from mri.ingestion.config import Settings
from mri.ingestion.manifest import new_run_id
from mri.observability import ddl, metrics, runtime
from mri.observability.events import PipelineEvent
from mri.observability.store import EventLog

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2
LIVE_SYNC_NOTE = "sync requires POSTGRES_HOST/PORT/DB/USER/PASSWORD (see .env)"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apps.observability",
        description="Emit, inspect and sync the unified audit event log.",
    )
    parser.add_argument(
        "--events-dir",
        type=Path,
        default=None,
        help="JSONL event log directory (default: settings/observability dir)",
    )
    parser.add_argument(
        "--json", action="store_true", dest="as_json", help="emit machine-readable JSON"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    record = sub.add_parser(
        "record", help="write one structured event (used by the Airflow DAG hook)"
    )
    record.add_argument("--dag-run-id", default="", help="Airflow DAG run identity to correlate")
    record.add_argument("--pipeline", default="airflow", help="pipeline name")
    record.add_argument("--stage", default="dag_run", help="stage name")
    record.add_argument("--run-id", default="", help="application run id (default: generated)")
    record.add_argument("--status", default="success", help="event status")
    record.add_argument("--source", default="", help="source id")
    record.add_argument("--batch-id", default="", help="batch id")
    record.add_argument("--logical-date", default="", help="Airflow data interval start")
    record.add_argument("--message", default="", help="human-readable message")
    record.add_argument("--error-code", default="", help="machine error code")

    sync = sub.add_parser(
        "sync",
        help="load the JSONL event log into the PostgreSQL ops schema "
        "(creates tables and metric views)",
    )
    sync.add_argument("--schema", default=ddl.OPS_SCHEMA, help="target ops schema (default: ops)")

    sub.add_parser(
        "metrics",
        help="compute pipeline metrics from the JSONL event log (offline)",
    )

    trace = sub.add_parser(
        "trace",
        help="print the ordered events of one run, DAG run or batch across all stages",
    )
    trace.add_argument("--run-id", default="", help="application run id to trace")
    trace.add_argument("--dag-run-id", default="", help="Airflow DAG run id to trace")
    trace.add_argument("--batch-id", default="", help="batch id to trace")
    return parser


def _events_dir(args: argparse.Namespace) -> Path:
    if args.events_dir is not None:
        return Path(args.events_dir)
    return Settings.from_env().observability_dir


def _cmd_record(args: argparse.Namespace) -> int:
    runtime.set_context(events_dir=_events_dir(args))
    run_id = args.run_id or f"airflow-{new_run_id()}"
    payload: dict[str, object] = {}
    if args.logical_date:
        payload["logical_date"] = args.logical_date
    runtime.emit(
        pipeline=args.pipeline,
        stage=args.stage,
        run_id=run_id,
        dag_run_id=args.dag_run_id,
        status=args.status,
        source=args.source,
        batch_id=args.batch_id,
        error_code=args.error_code,
        message=args.message,
        payload=payload,
    )
    print(
        f"recorded {run_id} {args.pipeline}/{args.stage} status={args.status} "
        f"dag_run_id={args.dag_run_id or '-'}"
    )
    return EXIT_OK


def _cmd_metrics(args: argparse.Namespace) -> int:
    events = EventLog.resolve(_events_dir(args)).read_events()
    payload = metrics.aggregate(events)
    if args.as_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return EXIT_OK
    overall = payload["overall"]
    print(
        f"runs={overall['runs']} success_rate={overall['success_rate']}% "
        f"failure_rate={overall['failure_rate']}% "
        f"(succeeded={overall['succeeded']} failed={overall['failed']})"
    )
    for stage in payload["stages"]:
        print(
            f"  {stage['pipeline']}/{stage['stage']:<10} runs={stage['runs']:>3} "
            f"success={stage['success_rate']:>6}% rows_in={stage['rows_read']:>8} "
            f"rows_written={stage['rows_written']:>8} "
            f"rejected={stage['rows_rejected']:>5} avg_ms={stage['duration_ms_avg']:>9}"
        )
    for entry in payload["freshness"]:
        print(
            f"  freshness {entry['source']:<12} last={entry['last_successful_at']} "
            f"stale_s={entry['staleness_seconds']}"
        )
    return EXIT_OK


def _matches(event: PipelineEvent, args: argparse.Namespace) -> bool:
    if args.run_id and event.run_id != args.run_id:
        return False
    if args.dag_run_id and event.dag_run_id != args.dag_run_id:
        return False
    if args.batch_id and event.batch_id != args.batch_id:
        return False
    return True


def _cmd_trace(args: argparse.Namespace) -> int:
    if not (args.run_id or args.dag_run_id or args.batch_id):
        raise UsageError("trace requires --run-id, --dag-run-id or --batch-id")
    events = EventLog.resolve(_events_dir(args)).read_events()
    selected = sorted(
        (event for event in events if _matches(event, args)),
        key=lambda event: (event.timestamp, event.run_id, event.pipeline, event.stage),
    )
    if not selected:
        print("no events matched the selector", file=sys.stderr)
        return EXIT_FAILURE
    if args.as_json:
        print(json.dumps([event.as_dict() for event in selected], indent=2, ensure_ascii=False))
        return EXIT_OK
    for event in selected:
        target = event.batch_id or event.source or "-"
        counts = ""
        if event.records_read is not None or event.records_written is not None:
            counts = f" read={event.records_read} written={event.records_written}"
        duration = f" {event.duration_ms}ms" if event.duration_ms is not None else ""
        code = f" code={event.error_code}" if event.error_code else ""
        print(
            f"{event.timestamp} "
            f"[{event.pipeline}/{event.stage}] {event.status:<9} "
            f"run={event.run_id} target={target}{counts}{duration}{code}"
        )
    return EXIT_OK


def _sync_row(event: PipelineEvent) -> tuple[object, ...]:
    return (
        event.event_hash(),
        event.timestamp,
        event.run_id,
        event.dag_run_id,
        event.pipeline,
        event.stage,
        event.source,
        event.batch_id,
        event.status,
        event.duration_ms,
        event.records_read,
        event.records_written,
        event.records_rejected,
        event.error_code,
        event.message,
        json.dumps(dict(event.payload), ensure_ascii=False),
    )


def _cmd_sync(args: argparse.Namespace) -> int:
    import psycopg

    from mri.silver.loader import conninfo_for

    settings = Settings.from_env()
    if not settings.postgres_password:
        print(f"error: {LIVE_SYNC_NOTE}", file=sys.stderr)
        return EXIT_FAILURE
    events = EventLog.resolve(_events_dir(args)).read_events()
    try:
        connection = psycopg.connect(conninfo_for(settings))
    except psycopg.Error as exc:
        print(f"error: cannot connect to PostgreSQL: {exc}", file=sys.stderr)
        return EXIT_FAILURE
    table = ddl.qualified(args.schema, ddl.EVENT_TABLE)
    inserted = 0
    skipped = 0
    try:
        with connection as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    for statement in ddl.render_ops_ddl(args.schema):
                        cur.execute(statement)
            columns = (
                "event_hash, timestamp, run_id, dag_run_id, pipeline, stage, source, "
                "batch_id, status, duration_ms, records_read, records_written, "
                "records_rejected, error_code, message, payload"
            )
            placeholders = ", ".join(["%s"] * 15 + ["%s::jsonb"])
            statement = (
                f"INSERT INTO {table} ({columns}) VALUES ({placeholders}) "
                f"ON CONFLICT (event_hash) DO NOTHING"
            )
            with conn.cursor() as cur:
                for row in (_sync_row(event) for event in events):
                    cur.execute(statement, list(row))
                    if cur.rowcount == 1:
                        inserted += 1
                    else:
                        skipped += 1
    except psycopg.Error as exc:
        print(f"error: PostgreSQL error: {exc}", file=sys.stderr)
        return EXIT_FAILURE
    print(f"synced {len(events)} event(s): inserted={inserted} skipped={skipped} -> {args.schema}")
    return EXIT_OK


class UsageError(Exception):
    """Raised for command-line usage errors (exit code 2)."""


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    handlers = {
        "record": _cmd_record,
        "metrics": _cmd_metrics,
        "trace": _cmd_trace,
        "sync": _cmd_sync,
    }
    try:
        return handlers[args.command](args)
    except UsageError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
