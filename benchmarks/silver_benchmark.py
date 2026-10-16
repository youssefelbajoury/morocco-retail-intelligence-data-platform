"""Silver layer benchmark: promoted Bronze batches loaded into PostgreSQL.

Usage (from the repository root):

    uv run python benchmarks/silver_benchmark.py --scale small
    uv run python benchmarks/silver_benchmark.py --scale medium --skip-ingest --skip-bronze
    uv run python benchmarks/silver_benchmark.py --scale small --no-record

The benchmark is isolated from the live pipeline: it uses per-scale buckets
``retail-bench-raw-<scale>`` / ``retail-bench-bronze-<scale>`` and the bench
schemas ``raw_staging_bench`` / ``silver_bench`` / ``audit_bench``, which are
dropped (CASCADE) before every Silver load so each run measures a cold load.
``--skip-ingest`` reuses already uploaded raw objects and ``--skip-bronze``
reuses already promoted Bronze markers.  Results are appended to
``benchmarks/silver/results.json``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

import psycopg  # noqa: E402

from mri.ingestion.bronze import BronzePipeline  # noqa: E402
from mri.ingestion.config import Settings  # noqa: E402
from mri.ingestion.contracts import load_all_contracts  # noqa: E402
from mri.ingestion.exceptions import IngestionError, UsageError  # noqa: E402
from mri.ingestion.pipeline import IngestionPipeline  # noqa: E402
from mri.ingestion.storage import MinioObjectStore  # noqa: E402
from mri.silver import SilverLoader, conninfo_for  # noqa: E402

RESULTS_PATH = Path(__file__).resolve().parent / "silver" / "results.json"
SCALES = ("tiny", "small", "medium")
BENCH_SCHEMAS = ("staging", "silver", "audit")


def _store(settings: Settings) -> MinioObjectStore:
    if not settings.minio_access_key or not settings.minio_secret_key:
        raise SystemExit("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set (see .env)")
    return MinioObjectStore(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def _ingest(store: MinioObjectStore, settings: Settings, dataset: Path) -> None:
    pipeline = IngestionPipeline(store, settings, load_all_contracts())
    uploaded = 0
    total = 0
    for source in sorted(load_all_contracts()):
        source_dir = dataset / source
        if not source_dir.is_dir():
            continue
        result = pipeline.ingest_path(source_dir, source=source, recursive=True)
        total += len(result.records)
        uploaded += sum(1 for record in result.records if record.outcome == "ingested")
    print(f"  ingest: {uploaded}/{total} objects uploaded")


def _bucket_bytes(store: MinioObjectStore, bucket: str, prefix: str) -> int:
    if not store.bucket_exists(bucket):
        return 0
    return sum(len(store.get_bytes(bucket, key)) for key in store.list_keys(bucket, prefix))


def _drop_bench_schemas(settings: Settings) -> None:
    with psycopg.connect(conninfo_for(settings)) as connection:
        for schema in (
            settings.audit_schema,
            settings.silver_schema,
            settings.staging_schema,
        ):
            connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


def _schema_bytes(connection: object, schema: str) -> int:
    cursor = connection.execute(  # type: ignore[attr-defined]
        "SELECT coalesce(sum(pg_total_relation_size("
        "(quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass)), 0) "
        "FROM pg_tables WHERE schemaname = %s",
        (schema,),
    )
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def _database_bytes(connection: object) -> int:
    cursor = connection.execute("SELECT pg_database_size(current_database())")  # type: ignore[attr-defined]
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def run(scale: str, *, skip_ingest: bool, skip_bronze: bool) -> dict[str, object]:
    base = Settings.from_env()
    dataset = REPO_ROOT / "data" / "generated" / scale
    if not dataset.is_dir():
        raise SystemExit(f"dataset not found: {dataset} (run the generator first)")

    settings = replace(
        base,
        bucket_raw=f"retail-bench-raw-{scale}",
        bucket_bronze=f"retail-bench-bronze-{scale}",
        staging_schema="raw_staging_bench",
        silver_schema="silver_bench",
        audit_schema="audit_bench",
    )
    store = _store(settings)

    if not skip_ingest:
        print(f"ingesting {dataset} into {settings.bucket_raw} ...")
        _ingest(store, settings, dataset)

    if not skip_bronze:
        print(f"bronze run into {settings.bucket_bronze} ...")
        BronzePipeline(store, settings, load_all_contracts()).run()

    if not store.bucket_exists(settings.bucket_bronze):
        raise SystemExit(
            f"bronze bucket {settings.bucket_bronze!r} is missing (run without --skip-bronze)"
        )

    raw_bytes = _bucket_bytes(store, settings.bucket_raw, f"{settings.raw_prefix}/")
    bronze_bytes = _bucket_bytes(store, settings.bucket_bronze, f"{settings.bronze_prefix}/")

    print(f"dropping bench schemas ({settings.staging_schema}, {settings.silver_schema}, ...)")
    _drop_bench_schemas(settings)

    loader = SilverLoader(store, settings, load_all_contracts())
    print(f"silver load (scale={scale}) ...")
    started = time.perf_counter()
    result = loader.run()
    elapsed = time.perf_counter() - started

    if not result.ok:
        for record in result.failed:
            print(
                f"  failed {record.batch} -> {record.target_table}: {record.error}",
                file=sys.stderr,
            )
        raise SystemExit(2)

    with psycopg.connect(conninfo_for(settings)) as connection:
        schema_bytes = {
            "staging": _schema_bytes(connection, settings.staging_schema),
            "silver": _schema_bytes(connection, settings.silver_schema),
            "audit": _schema_bytes(connection, settings.audit_schema),
            "database_bytes": _database_bytes(connection),
        }

    rows_read = sum(record.rows_read for record in result.records)
    rows_inserted = sum(record.rows_inserted for record in result.records)
    rows_per_second = rows_inserted / elapsed if elapsed > 0 else 0.0
    timing = dict(result.manifest.get("timing", {}))

    entry: dict[str, object] = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "scale": scale,
        "skip_ingest": skip_ingest,
        "skip_bronze": skip_bronze,
        "batches": len(result.records),
        "counts": result.counts,
        "rows_read": rows_read,
        "rows_inserted": rows_inserted,
        "raw_bytes": raw_bytes,
        "bronze_bytes": bronze_bytes,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(rows_per_second, 1),
        "timing": timing,
        "bench_schema_bytes": schema_bytes,
        "reconciliation_balanced": result.manifest.get("reconciliation_balanced", True),
    }
    return entry


def _append(entry: dict[str, object]) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    history: list[dict[str, object]] = []
    if RESULTS_PATH.is_file():
        history = json.loads(RESULTS_PATH.read_text(encoding="utf-8"))
    history.append(entry)
    RESULTS_PATH.write_text(
        json.dumps(history, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark the Silver PostgreSQL load")
    parser.add_argument("--scale", choices=SCALES, default="small")
    parser.add_argument("--skip-ingest", action="store_true", help="reuse existing raw objects")
    parser.add_argument("--skip-bronze", action="store_true", help="reuse promoted bronze markers")
    parser.add_argument("--no-record", action="store_true", help="do not append results.json")
    args = parser.parse_args(argv)

    try:
        entry = run(args.scale, skip_ingest=args.skip_ingest, skip_bronze=args.skip_bronze)
    except (IngestionError, UsageError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if not args.no_record:
        _append(entry)

    print(
        f"  batches={entry['batches']} rows_read={entry['rows_read']} "
        f"inserted={entry['rows_inserted']} raw={entry['raw_bytes']}B "
        f"bronze={entry['bronze_bytes']}B elapsed={entry['elapsed_seconds']}s "
        f"rows/s={entry['rows_per_second']}"
    )
    print(f"  timing={entry['timing']} schema_bytes={entry['bench_schema_bytes']}")
    print(f"  counts={entry['counts']} balanced={entry['reconciliation_balanced']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
