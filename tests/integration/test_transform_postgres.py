"""Integration tests for the dbt analytics layer and SCD2 against live PostgreSQL.

Builds on the shared ``silver_env`` fixture: loads Silver once (idempotent),
builds the Phase 09 star schema in a uniquely suffixed ``warehouse_dbt_it*``
schema as the parity reference, then runs the full ``dbt build`` through the
repository wrapper (``python -m apps.dbt``) with throwaway ``dbt_*`` schemas.

Proves end to end: every mart row count and fact measure reconciles with
Silver, source freshness passes, rebuilds are deterministic (surrogate keys
stable), the guarded warehouse parity test passes inside the build, and a
real Silver attribute change opens a new SCD2 product version without
rewriting history or changing any fact surrogate key.
"""

from __future__ import annotations

import subprocess
import sys
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import psycopg
import pytest
from silver_fixtures import SilverEnv, drop_schemas

from mri.ingestion.config import Settings
from mri.silver import SilverLoader, conninfo_for
from mri.silver.ddl import qualified
from mri.warehouse import WarehouseBuilder, WarehouseBuildResult

REPO_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_TAIL = 6000


@dataclass(frozen=True)
class DbtEnv:
    """Session state: dbt settings, the Silver fixture and the first build log."""

    settings: Settings
    silver: SilverEnv
    suffix: str
    vars_yaml: str
    build_log: str


def _dbt(env: DbtEnv, *args: str) -> subprocess.CompletedProcess[str]:
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


@pytest.fixture(scope="session")
def dbt_env(silver_env: SilverEnv) -> Iterator[DbtEnv]:
    loader = SilverLoader(silver_env.store, silver_env.settings, silver_env.contracts)
    loader.run()
    loaded: dict[str, int] = {}
    with psycopg.connect(conninfo_for(silver_env.settings)) as conn:
        for source in sorted(silver_env.expected):
            count = conn.execute(
                f"SELECT count(*) FROM {qualified(silver_env.settings.silver_schema, source)}"
            ).fetchone()
            assert count is not None, source
            loaded[source] = int(count[0])
    assert loaded == silver_env.expected, loaded

    tag = uuid.uuid4().hex[:8]
    suffix = f"_it{tag}"
    warehouse_schema = f"warehouse_dbt_it{tag}"
    warehouse_settings = replace(silver_env.settings, warehouse_schema=warehouse_schema)
    warehouse_build: WarehouseBuildResult = WarehouseBuilder(warehouse_settings).build()
    assert warehouse_build.ok, warehouse_build.to_dict()

    env = DbtEnv(
        settings=warehouse_settings,
        silver=silver_env,
        suffix=suffix,
        vars_yaml=(
            f"{{silver_schema: {silver_env.settings.silver_schema}, "
            f"warehouse_schema: {warehouse_schema}, schema_suffix: {suffix}}}"
        ),
        build_log="",
    )
    build = _dbt(env, "build")
    if build.returncode != 0:
        pytest.fail(f"dbt build failed ({build.returncode}):\n{_tail(build)}")
    freshness = _dbt(env, "source", "freshness")
    if freshness.returncode != 0:
        pytest.fail(f"dbt source freshness failed:\n{_tail(freshness)}")
    env = replace(env, build_log=build.stdout)
    try:
        yield env
    finally:
        drop_schemas(
            silver_env.settings,
            warehouse_schema,
            f"dbt_staging{suffix}",
            f"dbt_intermediate{suffix}",
            f"dbt_marts{suffix}",
            f"dbt_snapshots{suffix}",
        )


def _rows(env: DbtEnv, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(conninfo_for(env.settings)) as conn:
        return conn.execute(sql, params).fetchall()


@pytest.fixture(name="env")
def _dbt_env_alias(dbt_env: DbtEnv) -> DbtEnv:
    return dbt_env


def _scalar(env: DbtEnv, sql: str, params: Sequence[Any] = ()) -> Any:
    rows = _rows(env, sql, params)
    return rows[0][0] if rows else None


def _mart(env: DbtEnv, table: str) -> str:
    return qualified(f"dbt_marts{env.suffix}", table)


def _silver(env: DbtEnv, table: str) -> str:
    return qualified(env.settings.silver_schema, table)


def test_dbt_build_completes_and_runs_every_test(env: DbtEnv) -> None:
    assert "Completed successfully" in env.build_log
    assert "assert_marts_match_warehouse" in env.build_log
    assert "ERROR" not in env.build_log.split("Completed")[0]


def test_every_mart_row_count_and_measure_reconciles_with_silver(env: DbtEnv) -> None:
    expected = env.silver.expected
    counts = {
        "dim_region": expected["regions"],
        "dim_canal": expected["channels"],
        "dim_date": expected["calendar"],
        "dim_magasin": expected["stores"],
        "dim_client": expected["customers"],
        "dim_produit": expected["products"],
        "dim_promotion": expected["promotions"],
        "fact_ventes": expected["sales"],
        "fact_achats": expected["purchases"],
        "fact_stock": expected["inventory"],
    }
    for table, wanted in counts.items():
        assert _scalar(env, f"SELECT count(*) FROM {_mart(env, table)}") == wanted, table

    fact_measures = {
        "fact_ventes": ("sales", ("quantity", "net_amount_mad", "cost_amount_mad", "margin_mad")),
        "fact_achats": ("purchases", ("quantity", "total_cost_mad")),
        "fact_stock": ("inventory", ("quantity_on_hand", "stock_value_mad")),
    }
    for table, (source, measures) in fact_measures.items():
        for measure in measures:
            silver_value = _scalar(
                env, f'SELECT round(sum("{measure}")::numeric, 2) FROM {_silver(env, source)}'
            )
            mart_value = _scalar(
                env, f'SELECT round(sum("{measure}")::numeric, 2) FROM {_mart(env, table)}'
            )
            assert silver_value == mart_value, f"{table}.{measure}"


def test_rebuild_is_deterministic_and_surrogate_keys_stay_stable(env: DbtEnv) -> None:
    def snapshot() -> dict[str, list[tuple[Any, ...]]]:
        return {
            "dim_produit": _rows(
                env,
                f"SELECT product_id, valid_from, valid_to, is_current, produit_sk "
                f"FROM {_mart(env, 'dim_produit')} ORDER BY product_id, valid_from",
            ),
            "dim_client": _rows(
                env,
                f"SELECT customer_id, client_sk FROM {_mart(env, 'dim_client')} "
                f"ORDER BY customer_id",
            ),
            "fact_ventes": _rows(
                env,
                f"SELECT count(*), md5(string_agg(produit_sk::text || ':' || sale_id, ',' "
                f"ORDER BY sale_id)) FROM {_mart(env, 'fact_ventes')}",
            ),
        }

    before = snapshot()
    rebuild = _dbt(env, "build")
    assert rebuild.returncode == 0, _tail(rebuild)
    assert snapshot() == before


def test_scd2_change_opens_new_version_without_rewriting_history(env: DbtEnv) -> None:
    expected_products = env.silver.expected["products"]
    product_id = _scalar(
        env,
        f"SELECT product_id FROM {_silver(env, 'sales')} "
        "GROUP BY product_id ORDER BY count(*) DESC LIMIT 1",
    )
    assert product_id is not None
    brand_before = _scalar(
        env, f"SELECT brand FROM {_silver(env, 'products')} WHERE product_id = %s", (product_id,)
    )
    versions_before = _scalar(env, f"SELECT count(*) FROM {_mart(env, 'dim_produit')}")
    current_before = _scalar(
        env, f"SELECT count(*) FROM {_mart(env, 'dim_produit')} WHERE is_current"
    )
    assert versions_before == expected_products
    assert current_before == expected_products

    def fact_signature() -> Any:
        return _scalar(
            env,
            f"SELECT count(*) || '|' || md5(string_agg(p.produit_sk::text, ',' "
            f"ORDER BY f.sale_id)) FROM {_mart(env, 'fact_ventes')} f "
            f"JOIN {_mart(env, 'dim_produit')} p ON p.produit_sk = f.produit_sk "
            f"WHERE p.product_id = %s",
            (product_id,),
        )

    signature_before = fact_signature()

    with psycopg.connect(conninfo_for(env.settings)) as conn:
        conn.execute(
            f"UPDATE {_silver(env, 'products')} SET brand = %s WHERE product_id = %s",
            ("ItBrandChanged", product_id),
        )

    rebuild = _dbt(env, "build")
    assert rebuild.returncode == 0, _tail(rebuild)

    versions_after = _scalar(env, f"SELECT count(*) FROM {_mart(env, 'dim_produit')}")
    current_after = _scalar(
        env, f"SELECT count(*) FROM {_mart(env, 'dim_produit')} WHERE is_current"
    )
    assert versions_after == expected_products + 1
    assert current_after == expected_products

    versions = _rows(
        env,
        f"SELECT brand, valid_from, valid_to, is_current "
        f"FROM {_mart(env, 'dim_produit')} WHERE product_id = %s ORDER BY valid_from",
        (product_id,),
    )
    assert len(versions) == 2
    old_brand, old_from, old_to, old_current = versions[0]
    new_brand, new_from, new_to, new_current = versions[1]
    assert (old_brand, old_current) == (brand_before, False)
    assert (new_brand, new_current) == ("ItBrandChanged", True)
    assert old_to is not None
    assert new_from == old_to
    assert new_to is None
    assert old_from is not None

    assert fact_signature() == signature_before
    assert (
        _scalar(
            env,
            f"SELECT count(*) FROM (SELECT product_id FROM {_mart(env, 'dim_produit')} "
            "GROUP BY product_id HAVING count(*) FILTER (WHERE is_current) <> 1) bad",
        )
        == 0
    )
