"""dbt incremental benchmark: full rebuild vs incremental runtime at scale.

Usage (from the repository root, after the container stack is up):

    uv run python benchmarks/dbt_benchmark.py --scale 100k
    uv run python benchmarks/dbt_benchmark.py --scale 1m --skip-generate --skip-ingest
    uv run python benchmarks/dbt_benchmark.py --scale 5m --no-record --delta-pct 1.0

The benchmark is isolated per scale: buckets ``retail-bench-raw-<scale>`` /
``retail-bench-bronze-<scale>``, the bench schemas ``raw_staging_bench`` /
``silver_bench_dbt<scale>`` / ``audit_bench_dbt<scale>`` and the dbt schemas
``dbt_*_dbt<scale>``.  Every phase after ``--skip-*`` reuses the previous state.

Pipeline measured:

1. **full**       ``dbt build --full-refresh``            (dims + snapshots + facts + tests)
2. **noop**       ``dbt run`` on the three facts           (steady state, nothing new)
3. **delta**      a synthetic late batch is appended to ``silver.sales`` (2% of the
                  sales rows; new PKs, watermark bumped) then ``dbt run`` applies it
4. **facts-full** ``dbt run --full-refresh`` on the facts only, used to prove the
                  incremental result equals a from-scratch rebuild (same footprint)

Peak RSS of the dbt process family is sampled continuously (``psutil``).  Rows per
second is computed against the rows a phase actually (re)built.  Results are
appended to ``benchmarks/dbt/results.json``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil
import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "src"))

from apps.dbt import build_command, build_env  # noqa: E402
from mri.ingestion.bronze import BronzePipeline  # noqa: E402
from mri.ingestion.config import Settings  # noqa: E402
from mri.ingestion.contracts import load_all_contracts  # noqa: E402
from mri.ingestion.exceptions import IngestionError, UsageError  # noqa: E402
from mri.ingestion.pipeline import IngestionPipeline  # noqa: E402
from mri.ingestion.storage import MinioObjectStore  # noqa: E402
from mri.silver import SilverLoader, conninfo_for  # noqa: E402
from mri.synthetic.generator import generate  # noqa: E402
from mri.synthetic.profiles import ScaleProfile  # noqa: E402
from mri.synthetic.writer import write_dataset  # noqa: E402

RESULTS_PATH = Path(__file__).resolve().parent / "dbt" / "results.json"
SCALES = ("100k", "1m", "5m")
DEFAULT_SEED = 4242
SALES_DELTA_PCT = 2.0

FACT_MODELS = ("fact_ventes", "fact_achats", "fact_stock")

FACT_MEASURES: dict[str, tuple[str, str]] = {
    "fact_ventes": ("quantity", "net_amount_mad"),
    "fact_achats": ("quantity", "total_cost_mad"),
    "fact_stock": ("quantity_on_hand", "stock_value_mad"),
}


def _profile(scale: str) -> ScaleProfile:
    """Dimension sets chosen so generation stays bounded at 1M/5M sales."""
    specs: dict[str, tuple[int, int, int, int, int, float, int]] = {
        "100k": (100_000, 120, 6, 800, 80, 0.5, 30),
        "1m": (1_000_000, 400, 16, 10_000, 500, 0.45, 14),
        "5m": (5_000_000, 1_000, 40, 60_000, 2_000, 0.4, 14),
    }
    sales, products, stores, customers, promotions, assortment, cadence = specs[scale]
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


def _store(settings: Settings) -> MinioObjectStore:
    if not settings.minio_access_key or not settings.minio_secret_key:
        raise SystemExit("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set (see .env)")
    return MinioObjectStore(
        settings.minio_endpoint,
        settings.minio_access_key,
        settings.minio_secret_key,
        secure=settings.minio_secure,
    )


def _generate_or_skip(profile: ScaleProfile, seed: int) -> Path:
    dataset = REPO_ROOT / "data" / "generated" / profile.name
    manifest_path = dataset / "manifest.json"
    if manifest_path.is_file():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("target_sales_rows") == profile.sales_rows:
            print(f"  dataset present: {dataset}")
            return dataset
    print(f"generating {profile.sales_rows:,} sales rows (profile {profile.name}) ...")
    started = time.perf_counter()
    tables = generate(seed, profile)
    write_dataset(tables, REPO_ROOT / "data" / "generated", profile, seed)
    print(f"  generated in {time.perf_counter() - started:.1f}s")
    return dataset


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


def _replace_settings(base: Settings, scale: str) -> Settings:
    name = f"dbt{scale}"
    return replace(
        base,
        bucket_raw=f"retail-bench-raw-{name}",
        bucket_bronze=f"retail-bench-bronze-{name}",
        staging_schema="raw_staging_bench",
        silver_schema=f"silver_bench_{name}",
        audit_schema=f"audit_bench_{name}",
    )


def _drop_bench_schemas(settings: Settings) -> None:
    name = settings.silver_schema.removeprefix("silver_bench_")
    schemas = [
        settings.audit_schema,
        settings.silver_schema,
        settings.staging_schema,
        f"warehouse_{name}",
        f"dbt_staging_{name}",
        f"dbt_intermediate_{name}",
        f"dbt_marts_{name}",
        f"dbt_snapshots_{name}",
    ]
    with psycopg.connect(conninfo_for(settings)) as connection:
        for schema in schemas:
            connection.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


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


def _silver_counts(settings: Settings) -> dict[str, int]:
    with psycopg.connect(conninfo_for(settings)) as connection:
        counts: dict[str, int] = {}
        for table in ("sales", "purchases", "inventory"):
            (row,) = connection.execute(
                f'SELECT count(*) FROM "{settings.silver_schema}".{table}'
            ).fetchone()
            counts[table] = int(row)
    return counts


def _fact_counts(settings: Settings, schema: str) -> dict[str, int]:
    with psycopg.connect(conninfo_for(settings)) as connection:
        counts: dict[str, int] = {}
        for model in FACT_MODELS:
            (row,) = connection.execute(f'SELECT count(*) FROM "{schema}".{model}').fetchone()
            counts[model] = int(row)
    return counts


def _fact_footprint(settings: Settings, schema: str) -> dict[str, dict[str, int | str]]:
    fingerprint: dict[str, dict[str, int | str]] = {}
    with psycopg.connect(conninfo_for(settings)) as connection:
        for model in FACT_MODELS:
            measure_a, measure_b = FACT_MEASURES[model]
            row = connection.execute(
                f"SELECT count(*), coalesce(sum({measure_a}::numeric), 0), "
                f'coalesce(sum({measure_b}::numeric), 0) FROM "{schema}".{model}'
            ).fetchone()
            fingerprint[model] = {
                "rows": int(row[0]),
                "sum_a": str(row[1]),
                "sum_b": str(row[2]),
            }
    return fingerprint


def _insert_sales_delta(settings: Settings, count: int) -> None:
    """Append `count` late sales rows (new PKs, watermark bumped) to silver.sales."""
    schema = settings.silver_schema
    with psycopg.connect(conninfo_for(settings)) as connection:
        connection.execute(
            f"CREATE TEMP TABLE _delta AS "
            f'SELECT * FROM "{schema}".sales '
            f"ORDER BY ingestion_timestamp DESC, sale_id LIMIT %s",
            (count,),
        )
        connection.execute(
            "UPDATE _delta SET sale_id = 'S' || lpad((10000000::bigint + rn)::text, 8, '0') "
            "FROM (SELECT ctid, row_number() OVER (ORDER BY sale_id) AS rn FROM _delta) r "
            "WHERE _delta.ctid = r.ctid"
        )
        connection.execute("UPDATE _delta SET ingestion_timestamp = now() + interval '1 hour'")
        connection.execute(f'INSERT INTO "{schema}".sales SELECT * FROM _delta')
        connection.execute("DROP TABLE _delta")
    print(f"  appended {count} late sales rows (watermark bumped)")


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
    returncode = process.wait()
    elapsed = time.perf_counter() - started
    return {
        "returncode": int(returncode),
        "elapsed_seconds": round(elapsed, 3),
        "peak_rss_bytes": peak,
        "tail": b"".join(chunks)[-6000:].decode("utf-8", errors="replace"),
    }


def _vars(settings: Settings) -> str:
    name = settings.silver_schema.removeprefix("silver_bench_")
    return (
        f"{{silver_schema: {settings.silver_schema}, "
        f"warehouse_schema: warehouse_{name}, "
        f"schema_suffix: _{name}}}"
    )


def _log(result: dict[str, Any], label: str) -> None:
    print(
        f"  [{label}] rc={result['returncode']} elapsed={result['elapsed_seconds']}s "
        f"peak_rss={result['peak_rss_bytes']:,}B"
    )
    if result["returncode"] != 0:
        print(result["tail"], file=sys.stderr)


def run(
    scale: str,
    *,
    skip_generate: bool,
    skip_ingest: bool,
    skip_bronze: bool,
    skip_silver: bool,
    delta_pct: float,
    seed: int,
) -> dict[str, Any]:
    profile = _profile(scale)
    base = Settings.from_env()
    settings = _replace_settings(base, scale)
    store = _store(settings)

    if not skip_generate:
        dataset = _generate_or_skip(profile, seed)
    else:
        dataset = REPO_ROOT / "data" / "generated" / profile.name
        if not (dataset / "manifest.json").is_file():
            raise SystemExit(f"dataset not found: {dataset} (run without --skip-generate)")

    if not skip_ingest and not dataset.is_dir():
        raise SystemExit(f"dataset not found: {dataset} (run without --skip-ingest)")
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

    print(f"dropping bench schemas for {settings.silver_schema} ...")
    _drop_bench_schemas(settings)

    if not skip_silver:
        loader = SilverLoader(store, settings, load_all_contracts())
        print(f"silver load (scale={scale}) ...")
        started = time.perf_counter()
        result = loader.run()
        silver_elapsed = time.perf_counter() - started
        if not result.ok:
            for record in result.failed:
                print(f"  failed {record.batch} -> {record.target_table}: {record.error}")
            raise SystemExit(2)
    else:
        silver_elapsed = None

    total_sales = _silver_counts(settings)
    print(f"  silver counts: {total_sales}")
    if silver_elapsed is not None:
        print(f"  silver load total={silver_elapsed:.1f}s")

    schema = f"dbt_marts_dbt{scale}"
    full = _run_dbt(settings, ["build", "--full-refresh", "--vars", _vars(settings)])
    _log(full, "full-refresh build")
    if full["returncode"] != 0:
        raise SystemExit(2)
    facts_full = _fact_counts(settings, schema)
    print(f"  full facts: {facts_full}")
    db_after_full = _database_bytes(settings)

    noop = _run_dbt(settings, ["run", "--select", *FACT_MODELS, "--vars", _vars(settings)])
    _log(noop, "incremental (no-op)")
    if noop["returncode"] != 0:
        raise SystemExit(2)

    delta_n = max(1, round(total_sales["sales"] * delta_pct / 100.0))
    _insert_sales_delta(settings, delta_n)
    silver_after_delta = _silver_counts(settings)

    inc = _run_dbt(settings, ["run", "--select", *FACT_MODELS, "--vars", _vars(settings)])
    _log(inc, "incremental (delta)")
    if inc["returncode"] != 0:
        raise SystemExit(2)
    facts_inc = _fact_counts(settings, schema)
    print(f"  incremented facts: {facts_inc}")
    footprint_inc = _fact_footprint(settings, schema)
    db_after_inc = _database_bytes(settings)

    facts_ref = _run_dbt(
        settings,
        ["run", "--full-refresh", "--select", *FACT_MODELS, "--vars", _vars(settings)],
    )
    _log(facts_ref, "facts full-refresh")
    if facts_ref["returncode"] != 0:
        raise SystemExit(2)
    footprint_ref = _fact_footprint(settings, schema)

    identical = footprint_inc == footprint_ref
    reconciled = (
        facts_inc["fact_ventes"] == silver_after_delta["sales"]
        and facts_inc["fact_achats"] == silver_after_delta["purchases"]
        and facts_inc["fact_stock"] == silver_after_delta["inventory"]
    )

    entry: dict[str, Any] = {
        "timestamp": datetime.now(UTC).isoformat(timespec="seconds"),
        "scale": scale,
        "profile": {field: getattr(profile, field) for field in ScaleProfile.__dataclass_fields__},
        "seed": seed,
        "delta_pct": delta_pct,
        "silver_counts": total_sales,
        "silver_counts_after_delta": silver_after_delta,
        "silver_load_seconds": silver_elapsed,
        "phases": {
            "full": {
                "elapsed_seconds": full["elapsed_seconds"],
                "peak_rss_bytes": full["peak_rss_bytes"],
                "rows_rebuilt": sum(facts_full.values()),
                "rows_per_second": round(sum(facts_full.values()) / full["elapsed_seconds"], 1),
            },
            "noop": {
                "elapsed_seconds": noop["elapsed_seconds"],
                "peak_rss_bytes": noop["peak_rss_bytes"],
            },
            "incremental": {
                "elapsed_seconds": inc["elapsed_seconds"],
                "peak_rss_bytes": inc["peak_rss_bytes"],
                "delta_rows": delta_n,
                "rows_per_second": round(delta_n / inc["elapsed_seconds"], 1),
            },
            "facts_full_refresh": {
                "elapsed_seconds": facts_ref["elapsed_seconds"],
                "peak_rss_bytes": facts_ref["peak_rss_bytes"],
                "rows_rebuilt": sum(facts_inc.values()),
                "rows_per_second": round(sum(facts_inc.values()) / facts_ref["elapsed_seconds"], 1),
            },
        },
        "facts_full": facts_full,
        "facts_incremental": facts_inc,
        "footprint_incremental": footprint_inc,
        "footprint_full_refresh": footprint_ref,
        "db_bytes": {
            "after_full": db_after_full,
            "after_incremental": db_after_inc,
            "marts_schema": _schema_bytes(settings, schema),
            "silver_schema": _schema_bytes(settings, settings.silver_schema),
            "database": _database_bytes(settings),
        },
        "equal_full_and_incremental": identical,
        "reconciliation_balanced": reconciled,
    }
    print(
        f"  full={full['elapsed_seconds']}s noop={noop['elapsed_seconds']}s "
        f"inc(delta={delta_n})={inc['elapsed_seconds']}s "
        f"facts_full={facts_ref['elapsed_seconds']}s"
    )
    print(f"  equal_full_and_incremental={identical} reconciled={reconciled}")
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
    parser = argparse.ArgumentParser(description="Benchmark dbt full vs incremental rebuilds")
    parser.add_argument("--scale", choices=SCALES, default="100k")
    parser.add_argument("--skip-generate", action="store_true", help="reuse generated dataset")
    parser.add_argument("--skip-ingest", action="store_true", help="reuse raw objects")
    parser.add_argument("--skip-bronze", action="store_true", help="reuse bronze markers")
    parser.add_argument("--skip-silver", action="store_true", help="reuse silver tables")
    parser.add_argument("--delta-pct", type=float, default=SALES_DELTA_PCT, help="delta size")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--no-record", action="store_true", help="do not append results.json")
    args = parser.parse_args(argv)

    try:
        entry = run(
            args.scale,
            skip_generate=args.skip_generate,
            skip_ingest=args.skip_ingest,
            skip_bronze=args.skip_bronze,
            skip_silver=args.skip_silver,
            delta_pct=args.delta_pct,
            seed=args.seed,
        )
    except (IngestionError, UsageError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if not args.no_record:
        _append(entry)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
