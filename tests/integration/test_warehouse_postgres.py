"""Integration tests for the star-schema warehouse against live PostgreSQL.

Builds on the shared ``silver_env`` fixture: loads Silver once (idempotent),
then builds the warehouse in a uniquely suffixed ``warehouse_it*`` schema and
proves the warehouse contract end to end — reconciliation of row counts and
measure sums, surrogate-key and business-key uniqueness, documented fact
grains, referential integrity of every foreign key, declared constraints and
indexes, ``dim_date`` coverage and attribute consistency, PostgreSQL-level
rejection of invalid rows, and deterministic rebuilds.

The warehouse schema is dropped again in the fixture teardown, so the suite
never touches the development ``warehouse`` schema.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from typing import Any

import psycopg
import pytest
from silver_fixtures import SilverEnv, drop_schemas

from mri.ingestion.config import Settings
from mri.silver import SilverLoader, conninfo_for
from mri.silver.ddl import qualified
from mri.warehouse import TABLES, WarehouseBuilder, WarehouseBuildResult
from mri.warehouse.schema import TABLE_BY_NAME


@dataclass(frozen=True)
class WarehouseEnv:
    """Session state: warehouse settings, the Silver fixture and the build."""

    settings: Settings
    silver: SilverEnv
    build: WarehouseBuildResult


@pytest.fixture(scope="session")
def warehouse_env(silver_env: SilverEnv) -> Iterator[WarehouseEnv]:
    loader = SilverLoader(silver_env.store, silver_env.settings, silver_env.contracts)
    loader.run()
    loaded: dict[str, int] = {}
    with psycopg.connect(conninfo_for(silver_env.settings)) as conn:
        for source in sorted({table.source for table in TABLES}):
            silver_table = qualified(silver_env.settings.silver_schema, source)
            count = conn.execute(f"SELECT count(*) FROM {silver_table}").fetchone()
            assert count is not None, source
            loaded[source] = int(count[0])
    expected = {source: silver_env.expected[source] for source in loaded}
    assert loaded == expected, loaded

    warehouse_schema = f"warehouse_it{uuid.uuid4().hex[:8]}"
    settings = replace(silver_env.settings, warehouse_schema=warehouse_schema)
    build = WarehouseBuilder(settings).build()
    assert build.ok, build.to_dict()
    try:
        yield WarehouseEnv(settings=settings, silver=silver_env, build=build)
    finally:
        drop_schemas(silver_env.settings, warehouse_schema)


def _rows(env: WarehouseEnv, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(conninfo_for(env.settings)) as conn:
        return conn.execute(sql, params).fetchall()


def _scalar(env: WarehouseEnv, sql: str) -> Any:
    rows = _rows(env, sql)
    return rows[0][0] if rows else None


def _wh(env: WarehouseEnv, table: str) -> str:
    return qualified(env.settings.warehouse_schema, table)


def _silver(env: WarehouseEnv, table: str) -> str:
    return qualified(env.settings.silver_schema, table)


FRESH_BUSINESS_KEYS: dict[str, dict[str, str]] = {
    "fact_ventes": {"sale_id": "'SZZZZZZZZZ'"},
    "fact_achats": {"purchase_id": "'PZZZZZZZZZ'"},
    "dim_client": {"customer_id": "'CU_TEST9999'"},
}


def _row_with_changes(env: WarehouseEnv, table: str, changes: dict[str, str]) -> str:
    spec = TABLE_BY_NAME[table]
    columns = [name for name in spec.column_names if name != spec.surrogate_key]
    overrides = {**FRESH_BUSINESS_KEYS.get(table, {}), **changes}
    column_list = ", ".join(f'"{name}"' for name in columns)
    select_list = ", ".join(overrides.get(name, f'"{name}"') for name in columns)
    target = _wh(env, table)
    return f"INSERT INTO {target} ({column_list}) SELECT {select_list} FROM {target} LIMIT 1"


def test_build_reconciles_every_table_and_measure_against_silver(
    warehouse_env: WarehouseEnv,
) -> None:
    result = warehouse_env.build
    assert result.ok
    assert len(result.reconciliation) == len(TABLES)
    assert {entry.target_table for entry in result.reconciliation} == {
        table.name for table in TABLES
    }
    expected = warehouse_env.silver.expected
    for entry in result.reconciliation:
        assert entry.silver_rows == expected[entry.source_table]
        assert entry.warehouse_rows == expected[entry.source_table]
        assert result.tables[entry.target_table] == expected[entry.source_table]
        if entry.target_table.startswith("fact_"):
            assert entry.measures
            assert all(measure.balanced for measure in entry.measures)
            assert all(measure.difference == 0.0 for measure in entry.measures)
        else:
            assert not entry.measures
    assert set(result.timing) == {
        "ddl_seconds",
        "load_seconds",
        "reconcile_seconds",
        "total_seconds",
    }


def test_dimensions_have_unique_surrogate_and_business_keys(
    warehouse_env: WarehouseEnv,
) -> None:
    expected = warehouse_env.silver.expected
    for table in TABLES:
        if table.kind != "dimension":
            continue
        surrogate = table.surrogate_key
        assert surrogate is not None
        business = ", ".join(f'"{key}"' for key in table.business_key)
        rows = _rows(
            warehouse_env,
            f'SELECT count(*), count(DISTINCT "{surrogate}"), '
            f"count(DISTINCT ({business})) FROM {_wh(warehouse_env, table.name)}",
        )
        total, distinct_surrogate, distinct_business = rows[0]
        assert total == expected[table.source]
        assert distinct_surrogate == total
        assert distinct_business == total


def test_facts_satisfy_their_documented_grain(warehouse_env: WarehouseEnv) -> None:
    checks = {
        "fact_ventes": 'count(*), count(DISTINCT "sale_id")',
        "fact_achats": 'count(*), count(DISTINCT "purchase_id")',
        "fact_stock": ('count(*), count(DISTINCT ("date_sk", "magasin_sk", "produit_sk"))'),
    }
    for table, expression in checks.items():
        rows = _rows(warehouse_env, f"SELECT {expression} FROM {_wh(warehouse_env, table)}")
        total, distinct = rows[0]
        assert total == distinct == warehouse_env.silver.expected[TABLE_BY_NAME[table].source]

    total, distinct = _rows(
        warehouse_env,
        f"SELECT count(*), count(DISTINCT (d.date, m.store_id, p.product_id)) "
        f"FROM {_wh(warehouse_env, 'fact_stock')} s "
        f"JOIN {_wh(warehouse_env, 'dim_date')} d ON d.date_sk = s.date_sk "
        f"JOIN {_wh(warehouse_env, 'dim_magasin')} m ON m.magasin_sk = s.magasin_sk "
        f"JOIN {_wh(warehouse_env, 'dim_produit')} p ON p.produit_sk = s.produit_sk",
    )[0]
    assert total == distinct == warehouse_env.silver.expected["inventory"]


def test_every_declared_foreign_key_resolves_without_orphans(
    warehouse_env: WarehouseEnv,
) -> None:
    for table in TABLES:
        for fk in table.foreign_keys:
            orphans = _scalar(
                warehouse_env,
                f"SELECT count(*) FROM {_wh(warehouse_env, table.name)} c "
                f"LEFT JOIN {_wh(warehouse_env, fk.ref_table)} p "
                f'ON p."{fk.ref_column}" = c."{fk.column}" '
                f'WHERE c."{fk.column}" IS NOT NULL AND p."{fk.ref_column}" IS NULL',
            )
            assert orphans == 0, f"{table.name}.{fk.column} has {orphans} orphans"


def test_declared_constraints_and_indexes_exist(warehouse_env: WarehouseEnv) -> None:
    schema = warehouse_env.settings.warehouse_schema
    tables = {
        str(row[0])
        for row in _rows(
            warehouse_env,
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (schema,),
        )
    }
    assert tables == {table.name for table in TABLES}

    constraints = _rows(
        warehouse_env,
        "SELECT conrelid::regclass::text, conname, contype FROM pg_constraint "
        "WHERE connamespace = %s::regnamespace",
        (schema,),
    )
    by_table: dict[str, dict[str, list[str]]] = {}
    for qualified_name, conname, contype in constraints:
        table_name = str(qualified_name).rsplit(".", 1)[-1]
        by_table.setdefault(table_name, {}).setdefault(str(contype), []).append(str(conname))

    expected_indexes: set[str] = set()
    for table in TABLES:
        kinds = by_table.get(table.name, {})
        assert len(kinds.get("p", [])) == 1, table.name
        assert len(kinds.get("u", [])) == len(table.unique_keys), table.name
        assert len(kinds.get("f", [])) == len(table.foreign_keys), table.name
        checks = set(kinds.get("c", []))
        expected_checks = {
            f"ck_{table.name}_{column.name}" for column in table.columns if column.check
        }
        assert expected_checks <= checks, table.name
        expected_indexes.add(f"{table.name}_pkey")
        for keys in table.unique_keys:
            expected_indexes.add(f"uq_{table.name}_{'_'.join(keys)}")
        if table.kind == "fact":
            for fk in table.foreign_keys:
                if fk.create_index:
                    expected_indexes.add(f"ix_{table.name}_{fk.column}")

    actual_indexes = {
        str(row[0])
        for row in _rows(
            warehouse_env,
            "SELECT indexname FROM pg_indexes WHERE schemaname = %s",
            (schema,),
        )
    }
    assert actual_indexes == expected_indexes


def test_totals_and_optional_nulls_match_silver_independently(
    warehouse_env: WarehouseEnv,
) -> None:
    fact_measures = {
        "fact_ventes": (
            "sales",
            ("quantity", "net_amount_mad", "cost_amount_mad", "margin_mad"),
        ),
        "fact_achats": ("purchases", ("quantity", "total_cost_mad")),
        "fact_stock": ("inventory", ("quantity_on_hand", "stock_value_mad")),
    }
    for table, (source, measures) in fact_measures.items():
        silver_table = _silver(warehouse_env, source)
        warehouse_table = _wh(warehouse_env, table)
        for measure in measures:
            silver_value = _scalar(
                warehouse_env,
                f'SELECT round(sum("{measure}")::numeric, 2) FROM {silver_table}',
            )
            warehouse_value = _scalar(
                warehouse_env,
                f'SELECT round(sum("{measure}")::numeric, 2) FROM {warehouse_table}',
            )
            assert silver_value == warehouse_value, f"{table}.{measure}"

    silver_sales = _silver(warehouse_env, "sales")
    ventes = _wh(warehouse_env, "fact_ventes")
    silver_nulls = _scalar(
        warehouse_env,
        f"SELECT count(*) FROM {silver_sales} WHERE customer_id IS NULL",
    )
    warehouse_nulls = _scalar(
        warehouse_env,
        f"SELECT count(*) FROM {ventes} WHERE client_sk IS NULL",
    )
    assert warehouse_nulls == silver_nulls

    silver_promotions = _scalar(
        warehouse_env,
        f"SELECT count(*) FROM {silver_sales} WHERE promotion_id IS NULL",
    )
    warehouse_promotions = _scalar(
        warehouse_env,
        f"SELECT count(*) FROM {ventes} WHERE promotion_sk IS NULL",
    )
    assert warehouse_promotions == silver_promotions


def test_dim_date_covers_the_calendar_and_attributes_are_consistent(
    warehouse_env: WarehouseEnv,
) -> None:
    shared = (
        "date, year, month, quarter, iso_week, weekday, weekday_name, is_weekend, "
        "is_holiday, holiday_name, is_ramadan, is_eid_al_fitr, is_eid_al_adha, season"
    )
    differences = _scalar(
        warehouse_env,
        f"SELECT count(*) FROM ("
        f"(SELECT {shared} FROM {_silver(warehouse_env, 'calendar')} "
        f"EXCEPT SELECT {shared} FROM {_wh(warehouse_env, 'dim_date')}) "
        f"UNION ALL "
        f"(SELECT {shared} FROM {_wh(warehouse_env, 'dim_date')} "
        f"EXCEPT SELECT {shared} FROM {_silver(warehouse_env, 'calendar')}) "
        ") d",
    )
    assert differences == 0

    dim_rows = _scalar(warehouse_env, f"SELECT count(*) FROM {_wh(warehouse_env, 'dim_date')}")
    assert dim_rows == warehouse_env.silver.expected["calendar"]

    assert (
        _scalar(
            warehouse_env,
            f"SELECT count(*) FROM {_wh(warehouse_env, 'dim_date')} "
            'WHERE "day" <> EXTRACT(DAY FROM "date")::BIGINT',
        )
        == 0
    )
    assert (
        _scalar(
            warehouse_env,
            f"SELECT count(*) FROM {_wh(warehouse_env, 'dim_date')} "
            'WHERE "is_weekend" IS DISTINCT FROM ("weekday" >= 5)',
        )
        == 0
    )
    assert (
        _scalar(
            warehouse_env,
            f"SELECT count(DISTINCT season) FROM {_wh(warehouse_env, 'dim_date')}",
        )
        == 4
    )

    for table, column in (
        ("fact_ventes", "sale_date"),
        ("fact_achats", "purchase_date"),
        ("fact_stock", "snapshot_date"),
    ):
        unresolved = _scalar(
            warehouse_env,
            f"SELECT count(*) FROM {_silver(warehouse_env, TABLE_BY_NAME[table].source)} s "
            f"WHERE NOT EXISTS (SELECT 1 FROM {_wh(warehouse_env, 'dim_date')} d "
            f'WHERE d."date" = s."{column}")',
        )
        assert unresolved == 0


def test_postgresql_rejects_invalid_rows(warehouse_env: WarehouseEnv) -> None:
    with psycopg.connect(conninfo_for(warehouse_env.settings)) as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {_wh(warehouse_env, 'dim_region')} "
                    '("region_id", "region_name", "demand_weight") '
                    "SELECT region_id, region_name, demand_weight "
                    f"FROM {_wh(warehouse_env, 'dim_region')} LIMIT 1"
                )

        with pytest.raises(psycopg.errors.UniqueViolation):
            with conn.transaction():
                conn.execute(
                    _row_with_changes(warehouse_env, "fact_ventes", {"sale_id": "'S000000999'"})
                )

        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with conn.transaction():
                conn.execute(
                    _row_with_changes(warehouse_env, "fact_ventes", {"date_sk": "999999999"})
                )

        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                conn.execute(_row_with_changes(warehouse_env, "fact_ventes", {"quantity": "0"}))

        with pytest.raises(psycopg.errors.NotNullViolation):
            with conn.transaction():
                conn.execute(
                    _row_with_changes(warehouse_env, "fact_ventes", {"net_amount_mad": "NULL"})
                )

        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                conn.execute(
                    _row_with_changes(
                        warehouse_env,
                        "dim_client",
                        {"gender": "'X'", "customer_id": "'CU_TEST_GENDER'"},
                    )
                )

        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                conn.execute(
                    _row_with_changes(
                        warehouse_env,
                        "fact_stock",
                        {"quantity_on_hand": "-1", "date_sk": "999999998"},
                    )
                )


def test_rebuild_is_deterministic_surrogate_keys_stay_stable(
    warehouse_env: WarehouseEnv,
) -> None:
    def snapshot() -> dict[str, list[tuple[Any, ...]]]:
        captured: dict[str, list[tuple[Any, ...]]] = {}
        for table in TABLES:
            if table.kind != "dimension":
                continue
            surrogate = table.surrogate_key
            assert surrogate is not None
            business = ", ".join(f'"{key}"' for key in table.business_key)
            captured[table.name] = _rows(
                warehouse_env,
                f'SELECT ({business}), "{surrogate}" FROM {_wh(warehouse_env, table.name)} '
                f"ORDER BY ({business})",
            )
        return captured

    before = snapshot()
    rebuilt = WarehouseBuilder(warehouse_env.settings).build()
    assert rebuilt.ok
    after = snapshot()
    assert before == after
    for table in TABLES:
        assert rebuilt.tables[table.name] == warehouse_env.build.tables[table.name]
