"""Integration tests for incremental dbt fact materializations on live PostgreSQL.

Builds a fully isolated Bronze/Silver environment (its own in-memory raw/bronze
buckets and ``*_inc`` PostgreSQL schemas) on top of the shared ``silver_env``
fixture, then proves the Phase 14 incremental strategy:

* a fresh incremental build over an empty schema inserts the full fact set and
  is byte-identical to a ``--full-refresh`` rebuild of the same Silver state;
* late-arriving rows are inserted without touching previously materialized rows;
* corrected rows are updated in place (same natural key, newer watermark);
* re-delivered duplicate batches are idempotent no-ops (and an unchanged loader
  re-run reports ``already_loaded``);
* purchases and stock snapshots follow the same incremental path.

The fixture never mutates the shared Silver schemas: Bronze and Silver run
against scratch buckets/schemas, and dbt builds into throwaway ``dbt_*_inc*``
schemas which are dropped in teardown.
"""

from __future__ import annotations

import subprocess
import sys
import time
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import pytest
from silver_fixtures import SilverEnv, drop_schemas

from mri.ingestion.bronze import BronzePipeline
from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract
from mri.ingestion.storage import InMemoryObjectStore
from mri.silver import SilverLoader, conninfo_for
from mri.silver.ddl import qualified

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_TAIL = 6000

LATE_SALE_ID = "S999999997"
LATE_PURCHASE_ID = "P999999997"


@dataclass(frozen=True)
class IncrementalEnv:
    """Session state: isolated settings, the shared Bronze store and dbt vars."""

    settings: Settings
    silver: SilverEnv
    store: InMemoryObjectStore
    contracts: dict[str, Contract]
    tag: str
    suffix: str
    vars_yaml: str
    baseline: dict[str, tuple[int, str]]


def _dbt(env: IncrementalEnv, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "apps.dbt", "--no-partial-parse", *args, "--vars", env.vars_yaml],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def _tail(result: subprocess.CompletedProcess[str]) -> str:
    combined = f"{result.stdout}\n{result.stderr}"
    return combined[-OUTPUT_TAIL:]


def _rows(env: IncrementalEnv, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(conninfo_for(env.settings)) as conn:
        return conn.execute(sql, params).fetchall()


def _scalar(env: IncrementalEnv, sql: str, params: Sequence[Any] = ()) -> Any:
    rows = _rows(env, sql, params)
    return rows[0][0] if rows else None


def _mart(env: IncrementalEnv, table: str) -> str:
    return qualified(f"dbt_marts{env.suffix}", table)


def _silver(env: IncrementalEnv, table: str) -> str:
    return qualified(env.settings.silver_schema, table)


def _read(env: IncrementalEnv, source: str) -> pd.DataFrame:
    return pd.read_csv(env.silver.root / "data" / "tiny" / source / f"{source}.csv")


def _fact_hash(
    env: IncrementalEnv, table: str, where: str = "true", params: Sequence[Any] = ()
) -> tuple[int, str]:
    count = int(_scalar(env, f"SELECT count(*) FROM {_mart(env, table)} WHERE {where}", params))
    if count == 0:
        return 0, "EMPTY"
    digest = str(
        _scalar(
            env,
            f"SELECT md5(string_agg(r, '|')) FROM ("
            f"SELECT row_to_json(q)::text AS r FROM "
            f"(SELECT * FROM {_mart(env, table)} WHERE {where} ORDER BY 1) q "
            f"ORDER BY 1) s",
            params,
        )
    )
    return count, digest


def _assert_reconciles(env: IncrementalEnv) -> None:
    fact_measures = {
        "fact_ventes": ("sales", ("quantity", "net_amount_mad", "cost_amount_mad", "margin_mad")),
        "fact_achats": ("purchases", ("quantity", "total_cost_mad")),
        "fact_stock": ("inventory", ("quantity_on_hand", "stock_value_mad")),
    }
    for table, (source, measures) in fact_measures.items():
        mart_count = int(_scalar(env, f"SELECT count(*) FROM {_mart(env, table)}"))
        silver_count = int(_scalar(env, f"SELECT count(*) FROM {_silver(env, source)}"))
        assert mart_count == silver_count, (table, mart_count, silver_count)
        for measure in measures:
            mart_value = float(
                _scalar(
                    env,
                    f'SELECT round(sum("{measure}")::numeric, 2) FROM {_mart(env, table)}',
                )
            )
            silver_value = float(
                _scalar(
                    env,
                    f'SELECT round(sum("{measure}")::numeric, 2) FROM {_silver(env, source)}',
                )
            )
            assert mart_value == silver_value, (table, measure, mart_value, silver_value)


def _promote(env: IncrementalEnv, source: str, filename: str, frame: pd.DataFrame) -> None:
    """Put a crafted CSV into the isolated raw bucket and run Bronze + Silver for it."""
    data = frame.to_csv(index=False).encode("utf-8")
    env.store.put_bytes(
        env.settings.bucket_raw,
        f"{env.settings.raw_prefix}/{source}/{filename}",
        data,
    )
    time.sleep(1.05)
    bronze = BronzePipeline(env.store, env.settings, env.contracts).run(sources=[source])
    assert all(record.promoted for record in bronze.records), [
        (record.raw_key, record.outcome, record.quality_status, record.reason)
        for record in bronze.records
    ]
    load = SilverLoader(env.store, env.settings, env.contracts).run(sources=[source])
    assert load.ok, [
        (record.batch, record.target_table, record.outcome, record.error) for record in load.failed
    ]


@pytest.fixture(scope="session")
def incremental_env(
    silver_env: SilverEnv, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[IncrementalEnv]:
    base = silver_env.settings
    tag = uuid.uuid4().hex[:8]
    suffix = f"_inc{tag}"
    root = tmp_path_factory.mktemp("incremental-it")
    settings = replace(
        base,
        bucket_raw=f"inc-raw-{tag}",
        bucket_bronze=f"inc-bronze-{tag}",
        staging_schema=f"raw_staging_itinc{tag}",
        silver_schema=f"silver_itinc{tag}",
        audit_schema=f"audit_itinc{tag}",
        manifest_dir=root / "manifests",
        bronze_manifest_dir=root / "bronze-manifests",
        silver_manifest_dir=root / "silver-manifests",
    )
    warehouse_schema = f"warehouse_inc{tag}"
    store: InMemoryObjectStore = silver_env.store
    for key in store.list_keys(base.bucket_raw, f"{base.raw_prefix}/"):
        store.put_bytes(settings.bucket_raw, key, store.get_bytes(base.bucket_raw, key))
    contracts = silver_env.contracts

    bronze = BronzePipeline(store, settings, contracts).run()
    assert bronze.manifest.get("promoted") == len(silver_env.tables), bronze.counts
    load = SilverLoader(store, settings, contracts).run()
    assert not load.failed
    assert load.manifest.get("reconciliation_balanced", False) is True

    env = IncrementalEnv(
        settings=replace(settings, warehouse_schema=warehouse_schema),
        silver=silver_env,
        store=store,
        contracts=contracts,
        tag=tag,
        suffix=suffix,
        vars_yaml=(
            f"{{silver_schema: {settings.silver_schema}, warehouse_schema: {warehouse_schema}, "
            f"schema_suffix: {suffix}}}"
        ),
        baseline={},
    )
    build = _dbt(env, "build")
    assert build.returncode == 0, _tail(build)
    baseline = {
        table: _fact_hash(env, table) for table in ("fact_ventes", "fact_achats", "fact_stock")
    }
    env = replace(env, baseline=baseline)
    try:
        yield env
    finally:
        drop_schemas(
            settings,
            warehouse_schema,
            f"dbt_staging{suffix}",
            f"dbt_intermediate{suffix}",
            f"dbt_marts{suffix}",
            f"dbt_snapshots{suffix}",
        )


def test_fresh_incremental_build_matches_full_refresh(incremental_env: IncrementalEnv) -> None:
    before = {
        table: _fact_hash(incremental_env, table)
        for table in ("fact_ventes", "fact_achats", "fact_stock")
    }
    assert before == incremental_env.baseline
    rebuild = _dbt(incremental_env, "build", "--full-refresh")
    assert rebuild.returncode == 0, _tail(rebuild)
    _assert_reconciles(incremental_env)
    assert {
        table: _fact_hash(incremental_env, table)
        for table in ("fact_ventes", "fact_achats", "fact_stock")
    } == before


def test_late_arriving_sale_is_inserted_without_touching_history(
    incremental_env: IncrementalEnv,
) -> None:
    env = incremental_env
    sales = _read(env, "sales")
    late = sales.iloc[[0]].copy()
    late.loc[late.index[0], "sale_id"] = LATE_SALE_ID
    before = _fact_hash(env, "fact_ventes")
    _promote(env, "sales", f"sales__late{env.tag}.csv", late)

    build = _dbt(env, "build")
    assert build.returncode == 0, _tail(build)

    total, _digest = _fact_hash(env, "fact_ventes")
    assert total == before[0] + 1
    remaining = _fact_hash(env, "fact_ventes", where="sale_id <> %s", params=(LATE_SALE_ID,))
    assert remaining == before
    _assert_reconciles(env)

    row = _rows(
        env,
        f"SELECT d.date_sk, m.magasin_sk, c.canal_sk, p.produit_sk, f.quantity, "
        f"round(f.net_amount_mad::numeric, 2) FROM {_mart(env, 'fact_ventes')} f "
        f"JOIN {_mart(env, 'dim_date')} d ON d.date_sk = f.date_sk "
        f"JOIN {_mart(env, 'dim_magasin')} m ON m.magasin_sk = f.magasin_sk "
        f"JOIN {_mart(env, 'dim_canal')} c ON c.canal_sk = f.canal_sk "
        f"JOIN {_mart(env, 'dim_produit')} p ON p.produit_sk = f.produit_sk "
        f"WHERE f.sale_id = %s",
        (LATE_SALE_ID,),
    )
    assert len(row) == 1


def test_corrected_sale_is_updated_in_place(incremental_env: IncrementalEnv) -> None:
    env = incremental_env
    sales = _read(env, "sales")
    original = sales.iloc[1]
    sale_id = str(original["sale_id"])
    quantity2 = int(original["quantity"]) * 2
    net2 = round(
        quantity2 * float(original["unit_price_mad"]) * (1 - float(original["discount_pct"])), 2
    )
    cost2 = round(float(original["cost_amount_mad"]) * 2, 2)
    margin2 = round(max(net2 - cost2, 0.0), 2)
    corrected = sales.iloc[[1]].copy()
    corrected.loc[corrected.index[0], "quantity"] = quantity2
    corrected.loc[corrected.index[0], "net_amount_mad"] = net2
    corrected.loc[corrected.index[0], "cost_amount_mad"] = cost2
    corrected.loc[corrected.index[0], "margin_mad"] = margin2
    before = _fact_hash(env, "fact_ventes")
    _promote(env, "sales", f"sales__corrected{env.tag}.csv", corrected)

    build = _dbt(env, "build")
    assert build.returncode == 0, _tail(build)

    total, _digest = _fact_hash(env, "fact_ventes")
    assert total == before[0]
    row_vals = _rows(
        env,
        f'SELECT "quantity", round(net_amount_mad::numeric, 2), '
        f"round(cost_amount_mad::numeric, 2), round(margin_mad::numeric, 2) "
        f"FROM {_mart(env, 'fact_ventes')} WHERE sale_id = %s",
        (sale_id,),
    )
    assert row_vals is not None
    assert len(row_vals) == 1
    values = row_vals[0]
    assert (int(values[0]), float(values[1]), float(values[2]), float(values[3])) == (
        quantity2,
        net2,
        cost2,
        margin2,
    )
    _assert_reconciles(env)

    facts = ("fact_ventes", "fact_achats", "fact_stock")
    fingerprints = {table: _fact_hash(env, table) for table in facts}
    rebuild = _dbt(env, "build", "--full-refresh")
    assert rebuild.returncode == 0, _tail(rebuild)
    assert {table: _fact_hash(env, table) for table in facts} == fingerprints


def test_duplicate_batch_is_a_noop_and_rerun_is_idempotent(
    incremental_env: IncrementalEnv,
) -> None:
    env = incremental_env
    sales = _read(env, "sales")
    duplicate = sales.iloc[[2, 3]].copy()
    before = _fact_hash(env, "fact_ventes")
    _promote(env, "sales", f"sales__duplicate{env.tag}.csv", duplicate)

    build = _dbt(env, "build")
    assert build.returncode == 0, _tail(build)
    after_dup = _fact_hash(env, "fact_ventes")
    assert after_dup[0] == before[0]  # count unchanged
    _assert_reconciles(env)

    load = SilverLoader(env.store, env.settings, env.contracts).run(sources=["sales"])
    assert load.ok
    assert all(record.outcome == "already_loaded" for record in load.records), [
        (record.batch, record.outcome) for record in load.records
    ]
    second = _dbt(env, "build")
    assert second.returncode == 0, _tail(second)
    assert _fact_hash(env, "fact_ventes")[0] == before[0]


def test_late_purchase_and_new_stock_snapshot_are_inserted(
    incremental_env: IncrementalEnv,
) -> None:
    env = incremental_env
    purchases = _read(env, "purchases")
    late = purchases.iloc[[0]].copy()
    late.loc[late.index[0], "purchase_id"] = LATE_PURCHASE_ID
    before_achats = _fact_hash(env, "fact_achats")
    _promote(env, "purchases", f"purchases__late{env.tag}.csv", late)

    build = _dbt(env, "build")
    assert build.returncode == 0, _tail(build)
    total, _digest = _fact_hash(env, "fact_achats")
    assert total == before_achats[0] + 1
    _assert_reconciles(env)

    pairs = _rows(
        env,
        f"WITH pairs AS (SELECT DISTINCT store_id, product_id "
        f"FROM {_silver(env, 'inventory')}), "
        f"stores AS (SELECT DISTINCT store_id FROM {_silver(env, 'inventory')}), "
        f"prods AS (SELECT product_id, max(snapshot_date) AS sd "
        f"FROM {_silver(env, 'inventory')} GROUP BY product_id) "
        f"SELECT s.store_id, p.product_id, p.sd::text "
        f"FROM stores s CROSS JOIN prods p "
        f"WHERE NOT EXISTS (SELECT 1 FROM pairs q "
        f"WHERE q.store_id = s.store_id AND q.product_id = p.product_id) "
        f"ORDER BY p.product_id, s.store_id LIMIT 1",
    )
    assert pairs, "no un-inventoried (store, product) pair to add"
    store_id, product_id, snapshot_date = pairs[0]
    inventory = _read(env, "inventory")
    extra = inventory.iloc[[0]].copy()
    extra.loc[extra.index[0], "store_id"] = store_id
    extra.loc[extra.index[0], "product_id"] = product_id
    extra.loc[extra.index[0], "snapshot_date"] = snapshot_date
    before_stock = _fact_hash(env, "fact_stock")
    _promote(env, "inventory", f"inventory__extra{env.tag}.csv", extra)

    build = _dbt(env, "build")
    assert build.returncode == 0, _tail(build)
    total, _digest = _fact_hash(env, "fact_stock")
    assert total == before_stock[0] + 1
    _assert_reconciles(env)


__all__ = ["IncrementalEnv", "incremental_env"]
