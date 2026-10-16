"""Unit tests for the star-schema warehouse specification, DDL and CLI.

These tests need no database: they pin the design contract of the warehouse
package — the seven required dimensions and three required facts, documented
grains/business keys/surrogate keys, the no-fabrication column policy against
the data contracts, reconciliation metric definitions, rendered DDL (PKs,
UNIQUE, CHECK, FKs, identity columns, index set), deterministic load order,
the truncation statement and the ``plan``/usage behaviour of the CLI.
"""

from __future__ import annotations

import json

import pytest

from mri.ingestion.contracts import load_all_contracts
from mri.ingestion.exceptions import WarehouseError
from mri.warehouse import (
    DIMENSIONS,
    FACTS,
    TABLES,
    MeasureCheck,
    ReconciliationEntry,
    WarehouseBuildResult,
    render_load_statements,
    render_truncate_statement,
    render_warehouse_ddl,
)
from mri.warehouse.cli import EXIT_OK, EXIT_USAGE, main
from mri.warehouse.schema import (
    DERIVED_COLUMNS,
    DIMENSION_ORDER,
    FACT_ORDER,
    RECONCILIATION_MEASURES,
    TABLE_BY_NAME,
    MeasureMetric,
    ident,
    qualified,
)

REQUIRED_DIMENSIONS = {
    "dim_date",
    "dim_client",
    "dim_produit",
    "dim_magasin",
    "dim_region",
    "dim_canal",
    "dim_promotion",
}
REQUIRED_FACTS = {"fact_ventes", "fact_stock", "fact_achats"}


def test_star_schema_contains_the_required_dimensions_and_facts() -> None:
    assert {table.name for table in DIMENSIONS} == REQUIRED_DIMENSIONS
    assert {table.name for table in FACTS} == REQUIRED_FACTS
    assert len(TABLES) == 10
    assert DIMENSION_ORDER == tuple(table.name for table in DIMENSIONS)
    assert FACT_ORDER == tuple(table.name for table in FACTS)


def test_every_table_declares_grain_business_key_surrogate_key_and_source() -> None:
    for table in TABLES:
        assert table.grain, table.name
        assert table.business_key, table.name
        assert table.source, table.name
        if table.kind == "dimension":
            assert table.surrogate_key is not None, table.name
            assert table.surrogate_key in table.column_names
            assert table.primary_key == (table.surrogate_key,), table.name
            assert table.unique_keys, table.name
            assert table.business_key in table.unique_keys, table.name
        else:
            assert table.surrogate_key is None, table.name
            assert table.primary_key == table.business_key, table.name
            assert table.measures, table.name
        for key in table.business_key:
            assert key in table.column_names, table.name


def test_no_column_is_fabricated_beyond_contracts_sk_and_derived_columns() -> None:
    contracts = load_all_contracts()
    for table in TABLES:
        contract_columns = {column.name for column in contracts[table.source].columns}
        foreign_keys = {fk.column for fk in table.foreign_keys}
        allowed = contract_columns | foreign_keys | set(DERIVED_COLUMNS.get(table.name, {}))
        if table.surrogate_key is not None:
            allowed.add(table.surrogate_key)
        unknown = set(table.column_names) - allowed
        assert not unknown, f"{table.name} fabricates columns: {sorted(unknown)}"
        assert set(table.measures) <= contract_columns, table.name
        for degenerate in table.degenerate:
            assert degenerate in contract_columns, table.name


def test_reconciliation_metrics_are_real_fact_measure_columns() -> None:
    assert set(RECONCILIATION_MEASURES) == REQUIRED_FACTS
    contracts = load_all_contracts()
    for table_name, metrics in RECONCILIATION_MEASURES.items():
        table = TABLE_BY_NAME[table_name]
        contract_columns = {column.name for column in contracts[table.source].columns}
        assert metrics, table_name
        for metric in metrics:
            assert isinstance(metric, MeasureMetric)
            assert metric.column in table.measures, f"{table_name}.{metric.column}"
            assert metric.column in contract_columns, f"{table_name}.{metric.column}"
            assert metric.name == metric.column


def test_facts_share_conformed_dimensions() -> None:
    foreign_keys_by_table = {
        table.name: {fk.ref_table for fk in table.foreign_keys} for table in FACTS
    }
    ventes = foreign_keys_by_table["fact_ventes"]
    achats = foreign_keys_by_table["fact_achats"]
    stock = foreign_keys_by_table["fact_stock"]
    conformed = ventes & achats & stock
    assert {"dim_date", "dim_magasin", "dim_produit"} <= conformed
    assert any(fk.ref_table == "dim_client" for fk in TABLE_BY_NAME["fact_ventes"].foreign_keys)


def test_dimension_internal_foreign_keys_reference_real_dimensions() -> None:
    dimension_names = {table.name for table in DIMENSIONS}
    for table in TABLES:
        for fk in table.foreign_keys:
            assert fk.ref_table in dimension_names, f"{table.name}.{fk.column}"
            assert fk.ref_column in TABLE_BY_NAME[fk.ref_table].column_names
            assert fk.column.endswith("_sk"), f"{table.name}.{fk.column}"


def test_identity_surrogate_keys_are_exactly_the_seven_dimensions() -> None:
    identity_columns = {
        (table.name, column.name) for table in TABLES for column in table.columns if column.identity
    }
    expected = {(table.name, table.surrogate_key) for table in DIMENSIONS}
    assert identity_columns == expected
    for table in FACTS:
        for column in table.columns:
            assert not column.identity, f"{table.name}.{column.name}"


def test_identifiers_are_validated_and_quoted() -> None:
    assert ident("warehouse") == '"warehouse"'
    assert qualified("warehouse", "dim_date") == '"warehouse"."dim_date"'
    for bad in ("bad-name", "1abc", "a b", "", "tab;"):
        with pytest.raises(WarehouseError):
            ident(bad)


def test_warehouse_ddl_declares_schema_tables_keys_checks_and_index_set() -> None:
    statements = render_warehouse_ddl(warehouse_schema="wh", silver_schema="silver")
    assert statements[0] == 'CREATE SCHEMA IF NOT EXISTS "wh"'
    assert len(statements) > len(TABLES)

    table_ddl = [statement for statement in statements if "CREATE TABLE" in statement]
    assert len(table_ddl) == len(TABLES)
    for table in TABLES:
        ddl = next(text for text in table_ddl if f'"{table.name}"' in text)
        assert "CREATE TABLE IF NOT EXISTS" in ddl
        assert "PRIMARY KEY" in ddl
        for key in table.primary_key:
            assert f'"{key}"' in ddl
        for keys in table.unique_keys:
            assert f"uq_{table.name}_{'_'.join(keys)}" in ddl
        for column in table.columns:
            if column.identity:
                assert "GENERATED ALWAYS AS IDENTITY" in ddl
            if column.check is not None:
                assert f"ck_{table.name}_{column.name}" in ddl

    fk_statements = [statement for statement in statements if 'ADD CONSTRAINT "fk_' in statement]
    assert len(fk_statements) == sum(len(table.foreign_keys) for table in TABLES)
    index_statements = [statement for statement in statements if "CREATE INDEX" in statement]
    expected_indexes = sum(1 for table in FACTS for fk in table.foreign_keys if fk.create_index)
    assert len(index_statements) == expected_indexes
    assert all("ix_fact_" in statement for statement in index_statements)
    assert not any("ix_dim_" in statement for statement in statements)


def test_truncate_statement_restarts_identity_and_cascades() -> None:
    statement = render_truncate_statement(warehouse_schema="wh")
    assert statement.startswith("TRUNCATE ")
    assert statement.endswith("RESTART IDENTITY CASCADE")
    for table in TABLES:
        assert f'"wh"."{table.name}"' in statement


def test_load_statements_are_dimension_first_and_deterministically_ordered() -> None:
    statements = render_load_statements(warehouse_schema="wh", silver_schema="silver")
    assert tuple(name for name, _ in statements) == (*DIMENSION_ORDER, *FACT_ORDER)
    for name, sql in statements:
        table = TABLE_BY_NAME[name]
        assert sql.startswith(f'INSERT INTO "wh"."{name}"'), name
        if table.kind == "dimension":
            assert "ORDER BY" in sql, name
    fact_sql = [sql for name, sql in statements if TABLE_BY_NAME[name].kind == "fact"]
    assert all("JOIN" in sql for sql in fact_sql)


def test_reconciliation_entries_report_balance_for_rows_and_measures() -> None:
    empty = ReconciliationEntry("fact_ventes", "sales", 10, 10, ())
    assert empty.balanced
    missing_row = ReconciliationEntry("fact_ventes", "sales", 10, 9, ())
    assert not missing_row.balanced
    good_measure = MeasureCheck("net_amount_mad", 1.5, 1.5, 0.0, True)
    assert ReconciliationEntry("fact_ventes", "sales", 10, 10, (good_measure,)).balanced
    bad_measure = MeasureCheck("net_amount_mad", 1.5, 2.5, -1.0, False)
    assert not ReconciliationEntry("fact_ventes", "sales", 10, 10, (bad_measure,)).balanced


def test_build_result_reports_balance_and_serialises() -> None:
    balanced = WarehouseBuildResult(
        run_id="r",
        started_at="s",
        finished_at="f",
        schemas={"warehouse": "wh", "silver": "silver"},
        tables={"dim_date": 1},
        reconciliation=(ReconciliationEntry("dim_date", "calendar", 1, 1, ()),),
        timing={"total_seconds": 0.1},
    )
    assert balanced.ok
    payload = balanced.to_dict()
    assert payload["balanced"] is True
    assert payload["tables"] == {"dim_date": 1}
    assert payload["reconciliation"][0]["target_table"] == "dim_date"

    unbalanced = WarehouseBuildResult(
        run_id="r",
        started_at="s",
        finished_at="f",
        schemas={"warehouse": "wh", "silver": "silver"},
        tables={"dim_date": 1},
        reconciliation=(ReconciliationEntry("dim_date", "calendar", 1, 0, ()),),
        timing={},
    )
    assert not unbalanced.ok
    assert unbalanced.to_dict()["balanced"] is False


def test_cli_plan_prints_every_statement_without_connecting(
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["plan"])
    assert code == EXIT_OK
    out = capsys.readouterr().out
    assert "warehouse plan for schema" in out
    assert "CREATE SCHEMA" in out
    assert "TRUNCATE" in out
    assert "INSERT INTO" in out


def test_cli_plan_json_is_machine_readable(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--json", "--schema", "analytics", "plan"])
    assert code == EXIT_OK
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "analytics"
    expected = len(render_warehouse_ddl(warehouse_schema="analytics", silver_schema="analytics"))
    assert len(payload["statements"]) > expected
    assert all(isinstance(statement, str) for statement in payload["statements"])


def test_cli_rejects_non_identifier_schema(capsys: pytest.CaptureFixture[str]) -> None:
    code = main(["--schema", "1bad-name", "plan"])
    assert code == EXIT_USAGE
    assert "error:" in capsys.readouterr().err


def test_cli_requires_a_subcommand() -> None:
    with pytest.raises(SystemExit) as excinfo:
        main([])
    assert excinfo.value.code == EXIT_USAGE
