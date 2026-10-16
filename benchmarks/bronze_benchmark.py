"""Bronze layer benchmark: input/output size, elapsed time and rows/second.

Usage (from the repository root, after ingesting a dataset):

    uv run python benchmarks/bronze_benchmark.py --scale small
    uv run python benchmarks/bronze_benchmark.py --scale medium --skip-ingest
    uv run python benchmarks/bronze_benchmark.py --scale medium --dry-run

``--skip-ingest`` assumes ``raw/<source>/`` objects already exist in the raw
bucket (re-running the benchmark does not re-upload).  ``--dry-run`` plans
without writing Parquet or running Great Expectations (throughput baseline).
Results are appended to ``benchmarks/bronze/results.json``.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from mri.ingestion.bronze import BronzePipeline  # noqa: E402
from mri.ingestion.config import Settings  # noqa: E402
from mri.ingestion.contracts import load_all_contracts  # noqa: E402
from mri.ingestion.exceptions import IngestionError, UsageError  # noqa: E402
from mri.ingestion.pipeline import IngestionPipeline  # noqa: E402
from mri.ingestion.storage import MinioObjectStore  # noqa: E402

RESULTS_PATH = Path(__file__).resolve().parent / "bronze" / "results.json"
SCALES = ("tiny", "small", "medium")


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


def _raw_bytes(store: MinioObjectStore, settings: Settings) -> int:
    return sum(
        len(store.get_bytes(settings.bucket_raw, key))
        for key in store.list_keys(settings.bucket_raw, f"{settings.raw_prefix}/")
    )


def _bronze_bytes(store: MinioObjectStore, settings: Settings) -> int:
    if not store.bucket_exists(settings.bucket_bronze):
        return 0
    keys = store.list_keys(settings.bucket_bronze, f"{settings.bronze_prefix}/")
    return sum(len(store.get_bytes(settings.bucket_bronze, key)) for key in keys)


def run(scale: str, *, skip_ingest: bool, dry_run: bool) -> dict[str, object]:
    settings = Settings.from_env()
    store = _store(settings)
    dataset = REPO_ROOT / "data" / "generated" / scale
    if not dataset.is_dir():
        raise SystemExit(f"dataset not found: {dataset} (run the generator first)")

    if not skip_ingest and not dry_run:
        print(f"ingesting {dataset} ...")
        _ingest(store, settings, dataset)

    input_bytes = _raw_bytes(store, settings)
    rows_before = _bronze_bytes(store, settings)

    pipeline = BronzePipeline(store, settings, load_all_contracts(), dry_run=dry_run)
    print(f"bronze run (scale={scale}, dry_run={dry_run}) ...")
    started = time.perf_counter()
    result = pipeline.run()
    elapsed = time.perf_counter() - started

    rows_in = sum(record.rows_in for record in result.records)
    output_bytes = _bronze_bytes(store, settings) - rows_before if not dry_run else 0
    rows_per_second = rows_in / elapsed if elapsed > 0 else 0.0

    entry: dict[str, object] = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "scale": scale,
        "dry_run": dry_run,
        "batches": len(result.records),
        "counts": result.counts,
        "quality": result.manifest.get("quality", {}),
        "rows_in": rows_in,
        "rows_kept": sum(record.rows_kept for record in result.records),
        "rows_rejected": sum(record.rows_rejected for record in result.records),
        "input_bytes": input_bytes,
        "output_bytes": output_bytes,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(rows_per_second, 1),
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
    parser = argparse.ArgumentParser(description="Benchmark the Bronze pipeline")
    parser.add_argument("--scale", choices=SCALES, default="small")
    parser.add_argument("--skip-ingest", action="store_true", help="reuse existing raw objects")
    parser.add_argument("--dry-run", action="store_true", help="plan without writing or GX")
    parser.add_argument("--no-record", action="store_true", help="do not append results.json")
    args = parser.parse_args(argv)

    try:
        entry = run(args.scale, skip_ingest=args.skip_ingest, dry_run=args.dry_run)
    except (IngestionError, UsageError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if not args.no_record:
        _append(entry)

    print(
        f"  batches={entry['batches']} rows_in={entry['rows_in']} "
        f"rejected={entry['rows_rejected']} "
        f"input={entry['input_bytes']}B output={entry['output_bytes']}B "
        f"elapsed={entry['elapsed_seconds']}s rows/s={entry['rows_per_second']}"
    )
    print(f"  quality={entry['quality']} counts={entry['counts']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
