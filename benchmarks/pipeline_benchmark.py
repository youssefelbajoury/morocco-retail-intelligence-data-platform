"""End-to-end pipeline benchmark: one measured pass over every stage.

Usage (from the repository root, after the container stack is up):

    uv run python benchmarks/pipeline_benchmark.py --scale small
    uv run python benchmarks/pipeline_benchmark.py --scale medium --warm-repeats 1
    uv run python benchmarks/pipeline_benchmark.py --scale 1m --skip-generate --no-record

Isolation: buckets ``retail-bench-raw-e2e-<scale>`` /
``retail-bench-bronze-e2e-<scale>`` and the schemas ``raw_staging_e2e_<scale>`` /
``silver_e2e_<scale>`` / ``audit_e2e_<scale>`` / ``warehouse_e2e_<scale>`` plus
the dbt schemas ``dbt_*_e2e_<scale>``.  The buckets are purged before the cold
pass and dropped again at the end (``--keep-buckets`` keeps them), so the
development schemas are never touched.

Stages measured per pass (one cold pass, then ``--warm-repeats`` warm passes):

1. **generate** synthetic dataset (cold only; skipped when already present)
2. **ingest_files** local files to Raw (content-addressed; warm runs dedupe)
3. **ingest_external** Open-Meteo / Frankfurter / Open Food Facts (offline,
   deterministic responses through ``httpx.MockTransport``)
4. **bronze** Raw to partitioned Parquet + Great Expectations (warm = no-op)
5. **silver** Bronze to PostgreSQL (``raw_staging`` then ``silver``) + audit
6. **warehouse** Silver to star schema + reconciliation (one transaction)
7. **dbt** ``dbt build --full-refresh`` (dims + snapshots + facts + tests)

Database/schema footprint, row counts and peak RSS are captured per pass and
the result is appended to ``benchmarks/pipeline/results.json``.
"""

from __future__ import annotations

import argparse
import json
import platform
import statistics
import subprocess
import sys
import threading
import time
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx
import psutil
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from apps.dbt import build_command, build_env  # noqa: E402
from mri.ingestion.bronze import BronzePipeline  # noqa: E402
from mri.ingestion.clients.base import FetchOptions  # noqa: E402
from mri.ingestion.clients.fx_client import SOURCE as FX_SOURCE  # noqa: E402
from mri.ingestion.clients.fx_client import FxClient  # noqa: E402
from mri.ingestion.clients.http import HttpClient, RetryPolicy  # noqa: E402
from mri.ingestion.clients.product_catalog_client import (  # noqa: E402
    SOURCE as PRODUCTS_SOURCE,
)
from mri.ingestion.clients.product_catalog_client import ProductCatalogClient  # noqa: E402
from mri.ingestion.clients.weather_client import (  # noqa: E402
    SOURCE_FORECAST,
    SOURCE_HISTORY,
    WeatherClient,
)
from mri.ingestion.config import Settings  # noqa: E402
from mri.ingestion.contracts import load_all_contracts  # noqa: E402
from mri.ingestion.exceptions import IngestionError, UsageError  # noqa: E402
from mri.ingestion.external import ExternalPipeline  # noqa: E402
from mri.ingestion.pipeline import IngestionPipeline  # noqa: E402
from mri.ingestion.storage import MinioObjectStore  # noqa: E402
from mri.silver import SilverLoader, conninfo_for  # noqa: E402
from mri.synthetic.generator import generate  # noqa: E402
from mri.synthetic.profiles import ScaleProfile, get_profile  # noqa: E402
from mri.synthetic.writer import write_dataset  # noqa: E402
from mri.warehouse.loader import WarehouseBuilder  # noqa: E402

RESULTS_PATH = Path(__file__).resolve().parent / "pipeline" / "results.json"

SCALES = ("small", "medium", "1m", "5m")
DEFAULT_SEED = 4242
STAGES = (
    "generate",
    "ingest_files",
    "ingest_external",
    "bronze",
    "silver",
    "warehouse",
    "dbt",
)

FACT_MODELS = ("fact_ventes", "fact_achats", "fact_stock")
SILVER_TABLES = ("sales", "purchases", "inventory")

# 1M/5M mirror benchmarks/dbt_benchmark.py so the input dataset is identical
# (the dbt1m dataset on disk is reused verbatim when present).
_DBT_STYLE_SPECS: dict[str, tuple[int, int, int, int, int, float, int]] = {
    "1m": (1_000_000, 400, 16, 10_000, 500, 0.45, 14),
    "5m": (5_000_000, 1_000, 40, 60_000, 2_000, 0.4, 14),
}

DAY = date(2024, 6, 1)
EXTERNAL_LOCATIONS = ("R01", "R02", "R03", "R04")
EXTERNAL_BARCODES = ("3017620422003", "737628064502", "5000112637922")


def scale_profile(scale: str) -> ScaleProfile:
    """Documented generator profile for small/medium, dbt-equivalent for 1m/5m."""
    if scale in {"small", "medium"}:
        return get_profile(scale)
    sales, products, stores, customers, promotions, assortment, cadence = _DBT_STYLE_SPECS[scale]
    return ScaleProfile(
        name=f"dbt{scale}",
        sales_rows=sales,
        product_count=products,
        store_count=stores,
        customer_count=customers,
        promotion_count=promotions,
        assortment_ratio=assortment,
        snapshot_days=cadence,
        chunk_rows=1_000_000,
    )


def bench_settings(base: Settings, scale: str) -> Settings:
    """Isolated buckets/schemas/manifests for one scale."""
    name = f"e2e-{scale}"
    reports = REPO_ROOT / "reports" / "bench" / name
    return replace(
        base,
        bucket_raw=f"retail-bench-raw-{name}",
        bucket_bronze=f"retail-bench-bronze-{name}",
        staging_schema=f"raw_staging_e2e_{scale}",
        silver_schema=f"silver_e2e_{scale}",
        audit_schema=f"audit_e2e_{scale}",
        warehouse_schema=f"warehouse_e2e_{scale}",
        manifest_dir=reports / "ingestion",
        bronze_manifest_dir=reports / "bronze",
        silver_manifest_dir=reports / "silver",
        observability_dir=reports / "observability",
    )


def _guard_isolation(settings: Settings, base: Settings) -> None:
    touched = {
        "staging_schema": (settings.staging_schema, base.staging_schema),
        "silver_schema": (settings.silver_schema, base.silver_schema),
        "audit_schema": (settings.audit_schema, base.audit_schema),
        "warehouse_schema": (settings.warehouse_schema, base.warehouse_schema),
    }
    for label, (bench, dev) in touched.items():
        if bench == dev:
            raise SystemExit(f"refusing to run: benchmark {label} equals dev schema {dev!r}")


def _store(settings: Settings) -> MinioObjectStore:
    if not settings.minio_access_key or not settings.minio_secret_key:
        raise SystemExit("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set (see .env)")
    return MinioObjectStore(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def _purge_buckets(settings: Settings, buckets: tuple[str, ...]) -> None:
    from minio import Minio
    from minio.deleteobjects import DeleteObject

    client = Minio(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )
    for bucket in buckets:
        if not client.bucket_exists(bucket):
            continue
        objects = [
            DeleteObject(obj.object_name) for obj in client.list_objects(bucket, recursive=True)
        ]
        if objects:
            errors = list(client.remove_objects(bucket, objects))
            if errors:
                raise SystemExit(f"bucket cleanup failed for {bucket}: {errors[0]}")
        client.remove_bucket(bucket)
        print(f"  purged bucket {bucket}")


def _dataset_ready(dataset: Path, profile: ScaleProfile) -> bool:
    manifest_path = dataset / "manifest.json"
    if not manifest_path.is_file():
        return False
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return False
    return manifest.get("target_sales_rows") == profile.sales_rows


def _dir_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _minor_mock_handler(request: httpx.Request) -> httpx.Response:
    path = request.url.path
    if path.startswith("/v1/archive") or path.startswith("/v1/forecast"):
        payload = {
            "latitude": 33.57,
            "longitude": -7.59,
            "daily": {
                "time": ["2024-06-01", "2024-06-02"],
                "weather_code": [0, 1],
                "temperature_2m_max": [26.4, 27.1],
                "temperature_2m_min": [15.0, 15.5],
                "precipitation_sum": [0.0, 0.2],
                "precipitation_hours": [0.0, 1.0],
                "wind_speed_10m_max": [12.0, 10.0],
            },
        }
        return httpx.Response(200, json=payload, request=request)
    if "/v2/rates" in path:
        rows = [
            {"date": "2024-06-01", "base": "EUR", "quote": "MAD", "rate": 10.7},
            {"date": "2024-06-01", "base": "EUR", "quote": "USD", "rate": 1.08},
            {"date": "2024-06-01", "base": "EUR", "quote": "GBP", "rate": 0.85},
        ]
        return httpx.Response(200, json=rows, request=request)
    if "/product/" in path:
        code = path.split("/product/")[-1].split(".")[0]
        payload = {
            "code": code,
            "errors": [],
            "product": {"product_name": f"External product {code}"},
        }
        return httpx.Response(200, json=payload, request=request)
    return httpx.Response(404, json={"error": "unexpected path"}, request=request)


def _external_clients(settings: Settings) -> list[Any]:
    http = HttpClient(
        user_agent=settings.external_user_agent,
        timeout=settings.external_timeout_seconds,
        retry=RetryPolicy(max_attempts=1),
        transport=httpx.MockTransport(_minor_mock_handler),
    )
    return [
        WeatherClient(
            http,
            archive_base_url="https://archive.test",
            forecast_base_url="https://forecast.test",
        ),
        FxClient(http, base_url="https://fx.test"),
        ProductCatalogClient(http, base_url="https://off.test"),
    ]


def _ingest_files(
    store: MinioObjectStore,
    settings: Settings,
    contracts: dict[str, Any],
    dataset: Path,
) -> dict[str, int]:
    pipeline = IngestionPipeline(store, settings, contracts)
    uploaded = reused = rows = 0
    for source in sorted(contracts):
        source_dir = dataset / source
        if not source_dir.is_dir():
            continue
        result = pipeline.ingest_path(source_dir, source=source, recursive=True)
        for record in result.records:
            if record.outcome == "ingested":
                uploaded += 1
            elif record.outcome == "already_ingested":
                reused += 1
            rows += int(record.row_count)
    return {"objects_uploaded": uploaded, "objects_reused": reused, "rows": rows}


def _ingest_external(store: MinioObjectStore, settings: Settings) -> dict[str, int]:
    options = FetchOptions(
        sources=(SOURCE_HISTORY, SOURCE_FORECAST, FX_SOURCE, PRODUCTS_SOURCE),
        date=DAY,
        forecast_days=7,
        locations=EXTERNAL_LOCATIONS,
        barcodes=EXTERNAL_BARCODES,
    )
    result = ExternalPipeline(store, settings, _external_clients(settings), today=DAY).run(options)
    return {
        "requests": len(result.records),
        "fetched": int(result.counts.get("fetched", 0)),
        "already_fetched": int(result.counts.get("already_fetched", 0)),
        "failed": len(result.failed),
    }


def _run_dbt(settings: Settings, argv: list[str]) -> dict[str, Any]:
    command = build_command([*argv, "--no-partial-parse"])
    started = time.perf_counter()
    process = subprocess.Popen(
        command,
        env=build_env(settings),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    assert process.stdout is not None

    chunks: list[bytes] = []

    def _drain() -> None:
        for block in iter(lambda: process.stdout.read(1 << 16), b""):
            chunks.append(block)

    reader = threading.Thread(target=_drain, daemon=True)
    reader.start()

    peak = 0
    try:
        me: psutil.Process | None = psutil.Process(process.pid)
    except psutil.Error:
        me = None
    while process.poll() is None:
        if me is not None:
            try:
                family = [me, *me.children(recursive=True)]
                peak = max(peak, *(child.memory_info().rss for child in family))
            except psutil.Error:
                pass
        time.sleep(0.1)
    reader.join()
    returncode = int(process.wait())
    elapsed = time.perf_counter() - started
    return {
        "returncode": returncode,
        "elapsed_seconds": round(elapsed, 3),
        "peak_rss_bytes": peak,
        "tail": b"".join(chunks)[-6000:].decode("utf-8", errors="replace"),
    }


def _dbt_vars(settings: Settings, suffix: str) -> str:
    return (
        f"{{silver_schema: {settings.silver_schema}, "
        f"warehouse_schema: {settings.warehouse_schema}, schema_suffix: {suffix}}}"
    )


def _database_bytes(settings: Settings) -> int:
    with psycopg.connect(conninfo_for(settings)) as connection:
        (row,) = connection.execute("SELECT pg_database_size(current_database())").fetchone()
    return int(row)


def _schema_bytes(settings: Settings, schema: str) -> int:
    with psycopg.connect(conninfo_for(settings)) as connection:
        (row,) = connection.execute(
            "SELECT coalesce(sum(pg_total_relation_size("
            "(quote_ident(schemaname) || '.' || quote_ident(tablename))::regclass)), 0) "
            "FROM pg_tables WHERE schemaname = %s",
            (schema,),
        ).fetchone()
    return int(row)


def _table_counts(settings: Settings, schema: str, tables: tuple[str, ...]) -> dict[str, int]:
    with psycopg.connect(conninfo_for(settings)) as connection:
        counts: dict[str, int] = {}
        for table in tables:
            (row,) = connection.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()
            counts[table] = int(row)
    return counts


def _drop_bench_schemas(settings: Settings, suffix: str) -> None:
    schemas = [
        settings.staging_schema,
        settings.silver_schema,
        settings.audit_schema,
        settings.warehouse_schema,
        f"dbt_staging{suffix}",
        f"dbt_intermediate{suffix}",
        f"dbt_marts{suffix}",
        f"dbt_snapshots{suffix}",
    ]
    with psycopg.connect(conninfo_for(settings)) as connection:
        for schema in schemas:
            connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


class _RssSampler:
    """Sample current-process RSS on a background thread for the whole pass."""

    def __init__(self) -> None:
        self.peak = 0
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> _RssSampler:
        self._thread = threading.Thread(target=self._sample, daemon=True)
        self._thread.start()
        return self

    def _sample(self) -> None:
        process = psutil.Process()
        while not self._stop.is_set():
            try:
                self.peak = max(self.peak, process.memory_info().rss)
            except psutil.Error:
                pass
            self._stop.wait(0.1)

    def __exit__(self, *exc_info: object) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()


def run_pass(
    *,
    dataset: Path,
    settings: Settings,
    store: MinioObjectStore,
    contracts: dict[str, Any],
    suffix: str,
    cold: bool,
    generate_seconds: float | None,
) -> dict[str, Any]:
    """Run one isolated pass and return its measurements."""
    stages: dict[str, float] = {}
    details: dict[str, Any] = {}
    marts_schema = f"dbt_marts{suffix}"

    with _RssSampler() as sampler:
        _drop_bench_schemas(settings, suffix)

        if cold and generate_seconds is not None:
            stages["generate"] = round(generate_seconds, 3)

        started = time.perf_counter()
        ingest = _ingest_files(store, settings, contracts, dataset)
        stages["ingest_files"] = round(time.perf_counter() - started, 3)
        details["ingest_files"] = ingest

        started = time.perf_counter()
        external = _ingest_external(store, settings)
        stages["ingest_external"] = round(time.perf_counter() - started, 3)
        details["ingest_external"] = external

        started = time.perf_counter()
        bronze = BronzePipeline(store, settings, contracts).run()
        stages["bronze"] = round(time.perf_counter() - started, 3)
        details["bronze"] = {
            "outcomes": bronze.counts,
            "promoted": int(bronze.manifest.get("promoted", 0)),
            "quality": bronze.manifest.get("quality", {}),
        }

        started = time.perf_counter()
        silver = SilverLoader(store, settings, contracts).run()
        stages["silver"] = round(time.perf_counter() - started, 3)
        details["silver"] = {"ok": silver.ok, "batches": len(silver.records)}

        started = time.perf_counter()
        warehouse = WarehouseBuilder(settings).build()
        stages["warehouse"] = round(time.perf_counter() - started, 3)
        details["warehouse"] = {
            "balanced": warehouse.ok,
            "timing": dict(warehouse.timing),
        }

        dbt = _run_dbt(
            settings,
            ["build", "--full-refresh", "--vars", _dbt_vars(settings, suffix)],
        )
        stages["dbt"] = dbt["elapsed_seconds"]
        details["dbt"] = {
            "returncode": dbt["returncode"],
            "peak_rss_bytes": dbt["peak_rss_bytes"],
        }

        db_bytes = {
            "database": _database_bytes(settings),
            "silver_schema": _schema_bytes(settings, settings.silver_schema),
            "warehouse_schema": _schema_bytes(settings, settings.warehouse_schema),
            "dbt_marts_schema": _schema_bytes(settings, marts_schema),
        }

    if not silver.ok:
        for record in silver.failed:
            print(f"  silver failed {record.batch} -> {record.target_table}")
        raise SystemExit(2)
    if dbt["returncode"] != 0:
        print(dbt["tail"], file=sys.stderr)
        raise SystemExit(2)

    silver_counts = _table_counts(settings, settings.silver_schema, SILVER_TABLES)
    dbt_counts = _table_counts(settings, marts_schema, FACT_MODELS)

    return {
        "cold": cold,
        "stages": stages,
        "details": details,
        "peak_rss_bytes": sampler.peak,
        "counts": {
            "silver": silver_counts,
            "warehouse": {name: int(n) for name, n in warehouse.tables.items()},
            "dbt_marts": dbt_counts,
        },
        "db_bytes": db_bytes,
    }


def _warm_median(passes: list[dict[str, Any]]) -> dict[str, float] | None:
    warm = [run for run in passes if not run["cold"]]
    if not warm:
        return None
    merged: dict[str, float] = {}
    for stage in STAGES:
        values = [run["stages"][stage] for run in warm if stage in run["stages"]]
        if values:
            merged[stage] = round(statistics.median(values), 3)
    return merged


def _reconciled(passes: list[dict[str, Any]]) -> dict[str, bool]:
    last = passes[-1]
    silver = last["counts"]["silver"]
    warehouse = last["counts"]["warehouse"]
    dbt = last["counts"]["dbt_marts"]
    mapping = {
        "fact_ventes": "sales",
        "fact_achats": "purchases",
        "fact_stock": "inventory",
    }
    rows_match = all(
        warehouse.get(fact) == silver.get(source) == dbt.get(fact)
        for fact, source in mapping.items()
    )
    balanced = all(run["details"]["warehouse"]["balanced"] for run in passes)
    dbt_ok = all(run["details"]["dbt"]["returncode"] == 0 for run in passes)
    return {"rows_match": rows_match, "warehouse_balanced": balanced, "dbt_tests_passed": dbt_ok}


def _environment() -> dict[str, Any]:
    memory = psutil.virtual_memory()
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "cpu_count": psutil.cpu_count(logical=True),
        "total_memory_bytes": int(memory.total),
    }


def run(
    scale: str,
    *,
    warm_repeats: int,
    seed: int,
    skip_generate: bool,
) -> dict[str, Any]:
    profile = scale_profile(scale)
    base = Settings.from_env()
    settings = bench_settings(base, scale)
    _guard_isolation(settings, base)
    store = _store(settings)
    contracts = load_all_contracts()
    suffix = f"_e2e_{scale}"

    dataset = REPO_ROOT / "data" / "generated" / profile.name
    generate_seconds: float | None = None
    if _dataset_ready(dataset, profile):
        print(f"dataset present: {dataset}")
    elif skip_generate:
        raise SystemExit(f"dataset not found: {dataset} (run without --skip-generate)")
    else:
        print(f"generating {profile.sales_rows:,} sales rows (profile {profile.name}) ...")
        started = time.perf_counter()
        write_dataset(generate(seed, profile), REPO_ROOT / "data" / "generated", profile, seed)
        generate_seconds = time.perf_counter() - started
        print(f"  generated in {generate_seconds:.1f}s")
    dataset_bytes = _dir_bytes(dataset)

    _purge_buckets(settings, (settings.bucket_raw, settings.bucket_bronze))

    passes: list[dict[str, Any]] = []
    for index in range(warm_repeats + 1):
        cold = index == 0
        label = "cold" if cold else f"warm #{index}"
        print(f"pass {label} (scale={scale}) ...")
        result = run_pass(
            dataset=dataset,
            settings=settings,
            store=store,
            contracts=contracts,
            suffix=suffix,
            cold=cold,
            generate_seconds=generate_seconds if cold else None,
        )
        passes.append(result)
        print(f"  stages: {result['stages']}  peak_rss={result['peak_rss_bytes']:,}B")

    reconciliation = _reconciled(passes)
    entry: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "scale": scale,
        "profile": {field: getattr(profile, field) for field in ScaleProfile.__dataclass_fields__},
        "seed": seed,
        "warm_repeats": warm_repeats,
        "dataset_bytes": dataset_bytes,
        "environment": _environment(),
        "cold": passes[0],
        "warm_median_seconds": _warm_median(passes),
        "passes": passes,
        "reconciliation": reconciliation,
    }
    print(f"  reconciliation: {reconciliation}")
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
    parser = argparse.ArgumentParser(description="Benchmark the end-to-end pipeline")
    parser.add_argument("--scale", choices=SCALES, default="small")
    parser.add_argument(
        "--warm-repeats",
        type=int,
        default=1,
        help="warm passes after the cold pass (default 1)",
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--skip-generate", action="store_true", help="reuse dataset")
    parser.add_argument("--keep-buckets", action="store_true", help="keep bench buckets")
    parser.add_argument("--no-record", action="store_true", help="do not append results.json")
    args = parser.parse_args(argv)

    try:
        entry = run(
            args.scale,
            warm_repeats=max(0, args.warm_repeats),
            seed=args.seed,
            skip_generate=args.skip_generate,
        )
    except (IngestionError, UsageError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    base = Settings.from_env()
    settings = bench_settings(base, args.scale)
    if not args.keep_buckets:
        _purge_buckets(settings, (settings.bucket_raw, settings.bucket_bronze))

    if not args.no_record:
        _append(entry)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
