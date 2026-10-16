"""Command-line interface for the file ingestion layer.

Usage examples::

    python -m apps.ingestion contracts
    python -m apps.ingestion validate --source sales --path data/raw/sales.csv
    python -m apps.ingestion ingest   --source sales --path data/raw/sales.csv
    python -m apps.ingestion ingest   --path data/raw/ --recursive
    python -m apps.ingestion manifest --list
    python -m apps.ingestion bronze   --source all
    python -m apps.ingestion bronze   --source sales --dry-run --json
    python -m apps.ingestion external --source all --dry-run --json
    python -m apps.ingestion external --source fx --date 2024-06-03
    python -m apps.ingestion external --source weather --location R01 --date 2024-06-01
    python -m apps.ingestion external --source products --barcode 3017620422003
    python -m apps.ingestion load     --source all
    python -m apps.ingestion load     --source sales --dry-run --json
    python -m apps.ingestion load     --source all --force
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Sequence
from datetime import date
from pathlib import Path

from mri.ingestion.bronze import BronzePipeline, BronzeRunResult
from mri.ingestion.clients import FxClient, ProductCatalogClient, WeatherClient
from mri.ingestion.clients.base import ExternalClient, FetchOptions
from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract, load_all_contracts
from mri.ingestion.exceptions import IngestionError, UsageError
from mri.ingestion.external import (
    ExternalPipeline,
    ExternalRunResult,
    load_internal_barcode_index,
    read_barcodes,
    resolve_sources,
)
from mri.ingestion.manifest import (
    list_manifests,
    new_run_id,
    read_manifest_local,
)
from mri.ingestion.pipeline import IngestionPipeline, RunResult
from mri.ingestion.storage import MinioObjectStore
from mri.observability import runtime as observability
from mri.silver import SilverLoader, SilverRunResult

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apps.ingestion",
        description="Validate and ingest CSV/JSON source files against versioned contracts.",
    )
    parser.add_argument(
        "--contracts-dir", type=Path, default=None, help="override contracts directory"
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
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="validate and upload files to the raw bucket")
    ingest.add_argument("--source", help="explicit source id (else inferred from filename)")
    ingest.add_argument("--path", type=Path, required=True, help="file or directory to ingest")
    ingest.add_argument("--recursive", action="store_true", help="recurse into sub-directories")
    ingest.add_argument(
        "--dry-run", action="store_true", help="validate and plan without uploading"
    )
    ingest.add_argument("--expect-version", help="fail unless the contract version matches")

    validate = sub.add_parser("validate", help="validate files without uploading")
    validate.add_argument("--source")
    validate.add_argument("--path", type=Path, required=True)
    validate.add_argument("--recursive", action="store_true")
    validate.add_argument("--expect-version")

    manifest = sub.add_parser("manifest", help="inspect locally written run manifests")
    manifest.add_argument("--run-id", help="print one manifest")
    manifest.add_argument("--list", action="store_true", help="list all manifests")

    external = sub.add_parser(
        "external",
        help="fetch keyless external APIs (Open-Meteo, Frankfurter, Open Food Facts)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  apps.ingestion external --source all --dry-run --json\n"
            "  apps.ingestion external --source weather --location Casablanca "
            "--date 2024-06-01\n"
            "  apps.ingestion external --source fx --start-date 2024-06-01 "
            "--end-date 2024-06-07\n"
            "  apps.ingestion external --source products "
            "--barcode 3017620422003 --barcode 5000112637922\n"
            "  apps.ingestion external --source products --barcode-file barcodes.txt "
            "--internal-products products.csv\n"
            "storage: raw_external/source=<source>/retrieved_date=<YYYY-MM-DD>/"
            "request_hash=<sha256>/{response,metadata}.json\n"
            "exit codes: 0 ok, 1 at least one fetch failed, 2 usage error"
        ),
    )
    external.add_argument(
        "--source",
        action="append",
        default=[],
        metavar="NAME",
        help="all | weather | weather-history | weather-forecast | fx | products "
        "(repeatable, default: all)",
    )
    external.add_argument("--date", metavar="YYYY-MM-DD", help="single target day")
    external.add_argument("--start-date", metavar="YYYY-MM-DD", help="range start")
    external.add_argument("--end-date", metavar="YYYY-MM-DD", help="range end")
    external.add_argument(
        "--location",
        action="append",
        default=[],
        metavar="SEL",
        help="region id/name or chief city, repeatable (weather only; default: all 12)",
    )
    external.add_argument("--limit", type=int, help="cap fetched units (locations/barcodes)")
    external.add_argument(
        "--barcode", action="append", default=[], metavar="GTIN", help="product barcode"
    )
    external.add_argument("--barcode-file", type=Path, help="text file with one barcode per line")
    external.add_argument("--forecast-days", type=int, default=7, help="forecast horizon (1-16)")
    external.add_argument(
        "--internal-products",
        type=Path,
        help="CSV with barcode,product_id for exact MATCHED/NEW mapping",
    )
    external.add_argument(
        "--dry-run", action="store_true", help="plan requests without network or writes"
    )

    bronze = sub.add_parser(
        "bronze",
        help="standardize raw batches into partitioned Parquet gated by Great Expectations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  apps.ingestion bronze --source all\n"
            "  apps.ingestion bronze --source sales --source regions\n"
            "  apps.ingestion bronze --batch raw/sales/sales__0123456789abcdef.csv\n"
            "  apps.ingestion --json bronze --source sales --dry-run\n"
            "storage: bronze/<source>/[year=YYYY/month=MM/]part-<batch>.parquet\n"
            "         bronze/_quality/<run_id>/<source>/<batch>.json (quality report)\n"
            "         bronze/_promoted/<source>/<batch>.json (pass-only promotion marker)\n"
            "         bronze/_rejected/<source>/<batch>.json (excluded rows, failed only)\n"
            "exit codes: 0 ok, 1 at least one batch failed, 2 usage error"
        ),
    )
    bronze.add_argument(
        "--source",
        action="append",
        default=[],
        metavar="NAME",
        help="contract source id | all (repeatable, default: all)",
    )
    bronze.add_argument(
        "--batch",
        action="append",
        default=[],
        metavar="RAW_KEY",
        help="limit to raw object key(s) (repeatable)",
    )
    bronze.add_argument(
        "--dry-run", action="store_true", help="standardize and plan without writing or running GX"
    )

    load = sub.add_parser(
        "load",
        help="load promoted Bronze batches into the PostgreSQL silver schemas",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  apps.ingestion load --source all\n"
            "  apps.ingestion load --source sales --source regions\n"
            "  apps.ingestion load --source all --dry-run --json\n"
            "  apps.ingestion load --source all --force\n"
            "schemas: raw_staging (bulk COPY landing) -> silver (constraints) -> "
            "audit (load_audit registry + reconciliation report)\n"
            "exit codes: 0 ok, 1 at least one batch failed or a table is "
            "unbalanced, 2 usage error"
        ),
    )
    load.add_argument(
        "--source",
        action="append",
        default=[],
        metavar="NAME",
        help="contract source id | all (repeatable, default: all)",
    )
    load.add_argument(
        "--dry-run",
        action="store_true",
        help="plan batches without connecting to PostgreSQL",
    )
    load.add_argument(
        "--force",
        action="store_true",
        help="reload batches that are already registered as success (refreshes registry)",
    )

    sub.add_parser("contracts", help="list loaded source contracts")
    return parser


def _settings(args: argparse.Namespace) -> Settings:
    return Settings.from_env(
        contracts_dir=args.contracts_dir,
        observability_dir=getattr(args, "observability_dir", None),
    )


def _load_contracts(settings: Settings) -> dict[str, Contract]:
    return load_all_contracts(settings.contracts_dir)


def _check_expected(contract: Contract, expected: str | None) -> None:
    if expected and contract.version != expected:
        raise IngestionError(
            f"contract {contract.id} version {contract.version} != expected {expected}"
        )


def _make_store(settings: Settings) -> MinioObjectStore:
    if not settings.minio_access_key or not settings.minio_secret_key:
        raise IngestionError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set (see .env) for ingestion"
        )
    return MinioObjectStore(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def _cmd_contracts(args: argparse.Namespace) -> int:
    settings = _settings(args)
    contracts = _load_contracts(settings)
    if args.as_json:
        payload = [
            {
                "id": c.id,
                "version": c.version,
                "formats": list(c.format_types),
                "primary_key": list(c.primary_key),
                "columns": [col.name for col in c.columns],
            }
            for c in contracts.values()
        ]
        print(json.dumps(payload, indent=2))
        return EXIT_OK
    print(f"{len(contracts)} contracts in {settings.contracts_dir}")
    for contract in contracts.values():
        print(
            f"  {contract.id:<12} v{contract.version}  "
            f"formats={','.join(contract.format_types)}  "
            f"pk={','.join(contract.primary_key)}  columns={len(contract.columns)}"
        )
    return EXIT_OK


def _cmd_validate(args: argparse.Namespace) -> int:
    settings = _settings(args)
    contracts = _load_contracts(settings)
    pipeline = IngestionPipeline(None, settings, contracts)
    run_id = new_run_id()
    started = time.perf_counter()
    observability.emit(
        pipeline="ingestion",
        stage="validate",
        run_id=run_id,
        status="started",
        source=args.source or "",
        message="validation started",
        payload={"path": str(args.path), "recursive": args.recursive},
    )
    reports = pipeline.validate_path(args.path, source=args.source, recursive=args.recursive)
    total_rows = 0
    for report in reports:
        ok = report.result.ok
        total_rows += report.result.row_count
        observability.emit(
            pipeline="ingestion",
            stage="validate",
            run_id=run_id,
            status=("success" if ok else "failed"),
            source=(report.result.contract_id or report.file.source) or "",
            batch_id=report.file.path.stem,
            records_read=report.result.row_count,
            error_code=("" if ok else "VALIDATION_FAILED"),
            message=(
                f"validated {report.file.path.name}: "
                f"{'OK' if ok else 'INVALID'} ({len(report.result.issues)} issues)"
            ),
        )
    if args.expect_version:
        for report in reports:
            if report.result.contract_version != args.expect_version:
                raise IngestionError(
                    f"{report.file.path.name}: contract version "
                    f"{report.result.contract_version} != expected {args.expect_version}"
                )

    if args.as_json:
        payload = [
            {
                "file": str(report.file.path),
                "source": report.file.source,
                "format": report.result.format,
                "ok": report.result.ok,
                "row_count": report.result.row_count,
                "issues": [
                    {"code": i.code, "message": i.message, "column": i.column, "row": i.row}
                    for i in report.result.issues
                ],
            }
            for report in reports
        ]
        print(json.dumps(payload, indent=2))
    else:
        for report in reports:
            status = "OK" if report.result.ok else "INVALID"
            print(
                f"[{status}] {report.file.path.name} "
                f"(source={report.result.contract_id}, rows={report.result.row_count})"
            )
            for issue in report.result.issues:
                location = ""
                if issue.column:
                    location += f" column={issue.column}"
                if issue.row is not None:
                    location += f" row={issue.row}"
                print(f"    - {issue.code}: {issue.message}{location}")
        valid = sum(1 for report in reports if report.result.ok)
        print(f"validated {len(reports)} file(s); {valid} valid, {len(reports) - valid} invalid")

    valid_count = sum(1 for report in reports if report.result.ok)
    observability.emit(
        pipeline="ingestion",
        stage="validate",
        run_id=run_id,
        status=("success" if valid_count == len(reports) else "failed"),
        records_read=total_rows,
        duration_ms=observability.elapsed_ms(started),
        error_code=("" if valid_count == len(reports) else "VALIDATION_FAILED"),
        message=f"validated {len(reports)} file(s): {valid_count} valid",
    )
    return EXIT_OK if valid_count == len(reports) else EXIT_FAILURE


def _summarise(result: RunResult) -> int:
    counts = result.manifest.get("counts", {})
    print(
        f"run {result.run_id} (dry_run={result.dry_run}): "
        f"ingested={counts.get('ingested', 0)} "
        f"already_ingested={counts.get('already_ingested', 0)} "
        f"rejected={counts.get('rejected', 0)}"
    )
    for record in result.records:
        detail = record.object_key or record.rejected_key or ""
        note = f" :: {record.reason}" if record.reason else ""
        print(f"  [{record.outcome}] {record.original_filename} -> {detail}{note}")
    if result.manifest_path is not None:
        print(f"manifest: {result.manifest_path}")
    return EXIT_FAILURE if result.rejected else EXIT_OK


def _cmd_ingest(args: argparse.Namespace) -> int:
    settings = _settings(args)
    contracts = _load_contracts(settings)
    if args.source and args.source in contracts:
        _check_expected(contracts[args.source], args.expect_version)
    store = None if args.dry_run else _make_store(settings)
    pipeline = IngestionPipeline(store, settings, contracts, dry_run=args.dry_run)
    run_id = new_run_id()
    started = time.perf_counter()
    observability.emit(
        pipeline="ingestion",
        stage="ingest",
        run_id=run_id,
        status="started",
        source=args.source or "",
        message=f"ingestion run {run_id} executing {args.path}",
        payload={"path": str(args.path), "recursive": args.recursive, "dry_run": args.dry_run},
    )
    result = pipeline.ingest_path(
        args.path, source=args.source, recursive=args.recursive, run_id=run_id
    )
    total_rows = 0
    for record in result.records:
        ok = record.outcome != "rejected"
        total_rows += record.row_count or 0
        observability.emit(
            pipeline="ingestion",
            stage="ingest",
            run_id=result.run_id,
            status=("success" if ok else "failed"),
            source=record.source,
            batch_id=Path(record.original_filename).stem,
            records_read=record.row_count,
            error_code=("" if ok else "INGEST_REJECTED"),
            message=f"{record.outcome}: {record.original_filename}",
            payload={
                "sha256": record.sha256,
                "size_bytes": record.size_bytes,
                "format": record.format,
            },
        )
    counts = result.manifest.get("counts", {})
    observability.emit(
        pipeline="ingestion",
        stage="ingest",
        run_id=result.run_id,
        status=("success" if not result.rejected else "failed"),
        records_read=total_rows,
        duration_ms=observability.elapsed_ms(started),
        error_code=("" if not result.rejected else "INGEST_REJECTED"),
        message=f"ingestion finished: {counts}",
        payload={"dry_run": args.dry_run, "counts": dict(counts)},
    )

    if args.as_json:
        print(json.dumps(result.manifest, indent=2, ensure_ascii=False))
    else:
        _summarise(result)
    return EXIT_FAILURE if result.rejected else EXIT_OK


def _cmd_manifest(args: argparse.Namespace) -> int:
    settings = _settings(args)
    if args.list or not args.run_id:
        paths = list_manifests(settings.manifest_dir)
        if args.as_json:
            print(json.dumps([str(p) for p in paths], indent=2))
        else:
            print(f"{len(paths)} manifest(s) in {settings.manifest_dir}")
            for path in paths:
                print(f"  {path.name}")
        return EXIT_OK
    payload = read_manifest_local(settings.manifest_dir, args.run_id)
    if args.as_json:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(
            f"run {payload.get('run_id')} files={payload.get('file_count')} "
            f"counts={payload.get('counts')}"
        )
        for record in payload.get("records", []):
            print(f"  [{record['outcome']}] {record['original_filename']}")
    return EXIT_OK


def _parse_iso_date(value: str | None, flag: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise UsageError(f"{flag} must be YYYY-MM-DD, got {value!r}") from None


def _external_options(args: argparse.Namespace) -> FetchOptions:
    barcodes = list(args.barcode)
    if args.barcode_file is not None:
        barcodes.extend(read_barcodes(args.barcode_file))
    if args.limit is not None and args.limit < 1:
        raise UsageError(f"--limit must be >= 1, got {args.limit}")
    return FetchOptions(
        sources=resolve_sources(args.source),
        date=_parse_iso_date(args.date, "--date"),
        start_date=_parse_iso_date(args.start_date, "--start-date"),
        end_date=_parse_iso_date(args.end_date, "--end-date"),
        locations=tuple(args.location),
        barcodes=tuple(barcodes),
        forecast_days=args.forecast_days,
        limit=args.limit,
        internal_products_path=args.internal_products,
    )


def _build_clients(settings: Settings, options: FetchOptions) -> list[ExternalClient]:
    user_agent = settings.external_user_agent
    timeout = settings.external_timeout_seconds
    attempts = settings.external_max_attempts
    return [
        WeatherClient(
            WeatherClient.default_http(
                user_agent=user_agent, timeout=timeout, max_attempts=attempts
            ),
            archive_base_url=settings.open_meteo_archive_base_url,
            forecast_base_url=settings.open_meteo_base_url,
        ),
        FxClient(
            FxClient.default_http(user_agent=user_agent, timeout=timeout, max_attempts=attempts),
            base_url=settings.frankfurter_base_url,
        ),
        ProductCatalogClient(
            ProductCatalogClient.default_http(
                user_agent=user_agent,
                timeout=timeout,
                max_attempts=attempts,
                min_interval=settings.open_food_facts_min_interval,
            ),
            base_url=settings.open_food_facts_base_url,
            internal_index=load_internal_barcode_index(options.internal_products_path),
        ),
    ]


def _summarise_external(result: ExternalRunResult) -> int:
    counts = result.counts
    print(
        f"external run {result.run_id} (dry_run={result.dry_run}): "
        f"fetched={counts.get('fetched', 0)} "
        f"already_fetched={counts.get('already_fetched', 0)} "
        f"failed={counts.get('failed', 0)} "
        f"planned={counts.get('planned', 0)}"
    )
    for record in result.records:
        detail = record.object_key or ""
        note = f" :: {record.reason}" if record.reason else ""
        print(
            f"  [{record.outcome}] {record.source} "
            f"request_hash={record.request_hash[:12]} -> {detail}{note}"
        )
    if result.manifest_path is not None:
        print(f"manifest: {result.manifest_path}")
    return EXIT_FAILURE if result.failed else EXIT_OK


def _cmd_external(args: argparse.Namespace) -> int:
    settings = _settings(args)
    options = _external_options(args)
    store = None if args.dry_run else _make_store(settings)
    clients = _build_clients(settings, options)
    pipeline = ExternalPipeline(store, settings, clients, dry_run=args.dry_run)
    run_id = f"ext-{new_run_id()}"
    started = time.perf_counter()
    observability.emit(
        pipeline="ingestion",
        stage="external",
        run_id=run_id,
        status="started",
        message=f"external run {run_id} requesting sources",
        payload={"dry_run": args.dry_run, "sources": list(options.sources)},
    )
    result = pipeline.run(options, run_id=run_id)
    for record in result.records:
        ok = record.outcome not in {"failed", "skipped"}
        observability.emit(
            pipeline="ingestion",
            stage="external",
            run_id=result.run_id,
            status=record.outcome,
            source=record.source,
            batch_id=record.request_hash[:16],
            error_code=("" if ok else "EXTERNAL_FAILED"),
            message=f"{record.outcome}: {record.source} hash={record.request_hash[:12]}",
            payload={
                "request_hash": record.request_hash,
                "endpoint": record.endpoint,
                "http_status": record.http_status,
                "attempts": record.attempts,
                "size_bytes": record.size_bytes,
                "object_key": record.object_key,
                "reason": record.reason,
            },
        )
    counts = result.counts
    observability.emit(
        pipeline="ingestion",
        stage="external",
        run_id=result.run_id,
        status=("success" if not result.failed else "failed"),
        duration_ms=observability.elapsed_ms(started),
        error_code=("" if not result.failed else "EXTERNAL_FAILED"),
        message=f"external finished: {counts}",
        payload={"dry_run": args.dry_run, "counts": dict(counts)},
    )

    if args.as_json:
        print(json.dumps(result.manifest, indent=2, ensure_ascii=False))
    else:
        _summarise_external(result)
    return EXIT_FAILURE if result.failed else EXIT_OK


def _summarise_bronze(result: BronzeRunResult) -> int:
    counts = result.counts
    quality = result.manifest.get("quality", {})
    print(
        f"bronze run {result.run_id} (dry_run={result.dry_run}): "
        f"bronzed={counts.get('bronzed', 0)} "
        f"already_bronzed={counts.get('already_bronzed', 0)} "
        f"failed={counts.get('failed', 0)} "
        f"planned={counts.get('planned', 0)} "
        f"quality passed={quality.get('passed', 0)} failed={quality.get('failed', 0)} "
        f"promoted={result.manifest.get('promoted', 0)}"
    )
    for record in result.records:
        note = f" :: {record.reason}" if record.reason else ""
        print(
            f"  [{record.outcome}] {record.source}/{record.batch} "
            f"rows={record.rows_in} kept={record.rows_kept} "
            f"rejected={record.rows_rejected} parts={len(record.part_keys)}{note}"
        )
    if result.manifest_path is not None:
        print(f"manifest: {result.manifest_path}")
    return EXIT_FAILURE if result.failed else EXIT_OK


def _cmd_bronze(args: argparse.Namespace) -> int:
    settings = _settings(args)
    contracts = _load_contracts(settings)
    if args.source and "all" in args.source:
        if len(args.source) > 1:
            raise UsageError("do not combine 'all' with other --source values")
        sources: list[str] | None = None
    else:
        sources = list(args.source)
    store = _make_store(settings)
    pipeline = BronzePipeline(store, settings, contracts, dry_run=args.dry_run)
    run_id = new_run_id()
    started = time.perf_counter()
    observability.emit(
        pipeline="bronze",
        stage="bronze",
        run_id=run_id,
        status="started",
        source=", ".join(sources) if sources else "all",
        message=f"bronze run {run_id} starting discovery",
        payload={"dry_run": args.dry_run, "sources": sources or ["all"]},
    )
    result = pipeline.run(sources=sources, only_keys=tuple(args.batch) or None, run_id=run_id)
    for record in result.records:
        ok = record.outcome in {"bronzed", "already_bronzed"}
        partition = next((key for key in record.part_keys if "year=" in key), "")
        observability.emit(
            pipeline="bronze",
            stage="bronze",
            run_id=result.run_id,
            status=("success" if ok else record.outcome),
            source=record.source,
            batch_id=record.batch,
            records_read=record.rows_in,
            records_written=record.rows_kept,
            records_rejected=record.rows_rejected,
            error_code=("" if ok else "BRONZE_FAILED"),
            message=f"{record.outcome}: {record.source}/{record.batch}",
            payload={
                "quality_status": record.quality_status,
                "promoted": record.promoted,
                "expectations_total": record.expectations_total,
                "expectations_passed": record.expectations_passed,
                "expectations_failed": record.expectations_failed,
                "partition": partition,
                "part_keys": list(record.part_keys),
                "reason": record.reason,
            },
        )
    counts = result.counts
    observability.emit(
        pipeline="bronze",
        stage="bronze",
        run_id=result.run_id,
        status=("success" if not result.failed else "failed"),
        duration_ms=observability.elapsed_ms(started),
        error_code=("" if not result.failed else "BRONZE_FAILED"),
        message=f"bronze finished: {counts}",
        payload={
            "dry_run": args.dry_run,
            "counts": dict(counts),
            "quality": dict(result.manifest.get("quality", {})),
        },
    )

    if args.as_json:
        print(json.dumps(result.manifest, indent=2, ensure_ascii=False))
    else:
        _summarise_bronze(result)
    return EXIT_FAILURE if result.failed else EXIT_OK


def _summarise_silver(result: SilverRunResult) -> int:
    counts = result.counts
    print(
        f"silver run {result.run_id} (dry_run={result.dry_run}): "
        f"loaded={counts.get('loaded', 0)} "
        f"already_loaded={counts.get('already_loaded', 0)} "
        f"failed={counts.get('failed', 0)} "
        f"excluded={counts.get('excluded', 0)} "
        f"planned={counts.get('planned', 0)}"
    )
    for record in result.records:
        note = f" :: {record.error}" if record.error else ""
        detail = ""
        if record.outcome == "loaded":
            detail = f" rows={record.rows_read} inserted={record.rows_inserted}"
        print(f"  [{record.outcome}] {record.source}/{record.batch}{detail}{note}")
    reconciliation = result.manifest.get("reconciliation")
    if reconciliation is not None:
        for entry in reconciliation:
            status = "OK" if entry.get("balanced") else "UNBALANCED"
            print(
                f"  [{status}] {entry.get('target_table')}: "
                f"bronze={entry.get('bronze_distinct_keys', '?')} "
                f"silver={entry.get('silver_rows', '?')}"
                + (f" error={entry['error']}" if "error" in entry else "")
            )
    timing = result.manifest.get("timing", {})
    print(
        f"timing: schema={timing.get('schema_seconds', 0.0)}s "
        f"read={timing.get('read_seconds', 0.0)}s "
        f"db={timing.get('db_seconds', 0.0)}s "
        f"reconcile={timing.get('reconcile_seconds', 0.0)}s "
        f"total={timing.get('total_seconds', 0.0)}s"
    )
    if result.manifest_path is not None:
        print(f"manifest: {result.manifest_path}")
    return EXIT_FAILURE if not result.ok else EXIT_OK


def _cmd_load(args: argparse.Namespace) -> int:
    settings = _settings(args)
    contracts = _load_contracts(settings)
    if args.source and "all" in args.source:
        if len(args.source) > 1:
            raise UsageError("do not combine 'all' with other --source values")
        sources: list[str] | None = None
    else:
        sources = list(args.source)
    store = _make_store(settings)
    loader = SilverLoader(store, settings, contracts, dry_run=args.dry_run)
    run_id = new_run_id()
    started = time.perf_counter()
    observability.emit(
        pipeline="silver",
        stage="silver",
        run_id=run_id,
        status="started",
        source=", ".join(sources) if sources else "all",
        message=f"silver load run {run_id} starting",
        payload={"dry_run": args.dry_run, "force": args.force, "sources": sources or ["all"]},
    )
    result = loader.run(sources=sources, force=args.force, run_id=run_id)
    total_read = total_written = total_rejected = 0
    for record in result.records:
        ok = record.outcome != "failed"
        total_read += record.rows_read
        total_written += record.rows_inserted
        total_rejected += record.rows_rejected
        observability.emit(
            pipeline="silver",
            stage="silver",
            run_id=result.run_id,
            status=("success" if ok else "failed"),
            source=record.source,
            batch_id=record.batch,
            records_read=record.rows_read,
            records_written=record.rows_inserted,
            records_rejected=record.rows_rejected,
            error_code=("" if ok else "SILVER_LOAD_FAILED"),
            message=f"{record.outcome}: {record.source}/{record.batch} -> {record.target_table}",
            payload={
                "target_table": record.target_table,
                "marker_key": record.marker_key,
                "error": record.error,
            },
        )
    ok = result.ok
    observability.emit(
        pipeline="silver",
        stage="silver",
        run_id=result.run_id,
        status=("success" if ok else "failed"),
        records_read=total_read,
        records_written=total_written,
        records_rejected=total_rejected,
        duration_ms=observability.elapsed_ms(started),
        error_code=("" if ok else "SILVER_LOAD_FAILED"),
        message=("silver load finished: all OK" if ok else "silver load finished: failures"),
    )

    if args.as_json:
        print(json.dumps(result.manifest, indent=2, ensure_ascii=False))
    else:
        _summarise_silver(result)
    return EXIT_OK if result.ok else EXIT_FAILURE


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    observability.set_context(
        dag_run_id=args.dag_run_id or "",
        events_dir=args.observability_dir or Settings.from_env().observability_dir,
    )
    handlers = {
        "contracts": _cmd_contracts,
        "validate": _cmd_validate,
        "ingest": _cmd_ingest,
        "manifest": _cmd_manifest,
        "external": _cmd_external,
        "bronze": _cmd_bronze,
        "load": _cmd_load,
    }
    try:
        return handlers[args.command](args)
    except IngestionError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
