"""Unit tests for the Silver layer: table specs, DDL rendering and batch planning.

Table specifications are derived from the fourteen source contracts (columns
are never invented), DDL is checked for the structural guarantees of each of
the three schemas, and the planning path runs against real Bronze promotion
markers while an unroutable PostgreSQL host proves that ``--dry-run`` never
touches the database.
"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest
from bronze_fixtures import (
    CONTRACTS,
    DEFAULT_TAG,
    make_settings,
    sales_row,
    seed_dimensions,
    seed_raw,
)

from mri.ingestion.bronze import BronzePipeline
from mri.ingestion.cli import build_parser
from mri.ingestion.config import Settings
from mri.ingestion.exceptions import SilverLoadError
from mri.ingestion.storage import InMemoryObjectStore
from mri.silver import SilverLoader, SilverRecord, SilverRunResult, conninfo_for, plan_batches
from mri.silver.ddl import (
    PG_TYPES,
    PROVENANCE_COLUMNS,
    SOURCE_TABLES,
    TABLE_LOAD_ORDER,
    build_table_specs,
    conflict_clause,
    ident,
    render_audit_ddl,
    render_schema_ddl,
    render_silver_ddl,
    render_staging_ddl,
)
from mri.silver.loader import PromotedBatch, _parse_marker

UNROUTABLE_HOST = "192.0.2.1"
PROVENANCE_NAMES = tuple(column.name for column in PROVENANCE_COLUMNS)


def silver_settings(tmp_path: Path) -> Settings:
    return replace(
        make_settings(tmp_path),
        postgres_host=UNROUTABLE_HOST,
        silver_manifest_dir=tmp_path / "silver" / "manifests",
    )


def promoted_store(tmp_path: Path) -> tuple[InMemoryObjectStore, Settings]:
    settings = make_settings(tmp_path)
    store = InMemoryObjectStore()
    seed_dimensions(store, settings)
    seed_raw(store, settings, "sales", [sales_row(), sales_row(sale_id="S000000002")])
    BronzePipeline(store, settings, CONTRACTS).run()
    return store, settings


def loader_for(tmp_path: Path, store: InMemoryObjectStore, *, dry_run: bool = True) -> SilverLoader:
    return SilverLoader(store, silver_settings(tmp_path), CONTRACTS, dry_run=dry_run)


def craft_marker(store: InMemoryObjectStore, settings: Settings, payload: object, key: str) -> None:
    store.put_bytes(
        settings.bucket_bronze,
        f"{settings.promoted_prefix}/{key}",
        json.dumps(payload).encode("utf-8"),
    )


def test_source_table_mapping_covers_every_contract() -> None:
    assert len(SOURCE_TABLES) == 14
    assert set(SOURCE_TABLES) == set(CONTRACTS)
    assert set(SOURCE_TABLES.values()) == set(TABLE_LOAD_ORDER)
    assert len(TABLE_LOAD_ORDER) == 13


def test_silver_columns_are_exactly_contract_columns_plus_provenance() -> None:
    specs = build_table_specs(CONTRACTS)
    assert set(specs) == set(TABLE_LOAD_ORDER)
    for spec in specs.values():
        contract_columns: list[str] = []
        for source in spec.sources:
            for column in CONTRACTS[source].columns:
                if column.name not in contract_columns:
                    contract_columns.append(column.name)
        assert set(spec.column_names) == set(contract_columns) | set(PROVENANCE_NAMES)
        assert spec.column_names[-len(PROVENANCE_NAMES) :] == PROVENANCE_NAMES
        for source in spec.sources:
            for column in CONTRACTS[source].columns:
                target = spec.column(column.name)
                assert target.pg_type == PG_TYPES[column.type]
        for provenance in PROVENANCE_COLUMNS:
            assert spec.column(provenance.name) == provenance


def test_weather_merges_both_sources_without_a_primary_key() -> None:
    weather = build_table_specs(CONTRACTS)["weather"]
    assert weather.primary_key == ()
    assert weather.sources == ("open_meteo_forecast", "open_meteo_history")
    assert weather.conflict_keys["open_meteo_history"] == ("region_id", "observation_date")
    assert weather.conflict_keys["open_meteo_forecast"] == ("region_id", "forecast_date")
    assert weather.column("observation_date").nullable
    assert weather.column("forecast_date").nullable
    assert {check.name for check in weather.checks} == {"ck_weather_dates"}


def test_single_source_tables_keep_their_contract_primary_key() -> None:
    specs = build_table_specs(CONTRACTS)
    assert specs["sales"].primary_key == ("sale_id",)
    assert specs["purchases"].primary_key == ("purchase_id",)
    assert specs["fx_rates"].primary_key == ("date", "base", "quote")
    assert specs["inventory"].primary_key == ("snapshot_date", "store_id", "product_id")


def test_fact_tables_declare_the_expected_foreign_keys() -> None:
    specs = build_table_specs(CONTRACTS)
    assert {(fk.column, fk.ref_table) for fk in specs["sales"].foreign_keys} == {
        ("store_id", "stores"),
        ("channel_id", "channels"),
        ("customer_id", "customers"),
        ("product_id", "products"),
        ("promotion_id", "promotions"),
    }
    assert {fk.column for fk in specs["stores"].foreign_keys} == {"region_id"}
    assert {fk.column for fk in specs["customers"].foreign_keys} == {
        "region_id",
        "preferred_channel_id",
    }
    assert {fk.column for fk in specs["weather"].foreign_keys} == {"region_id"}
    assert specs["regions"].foreign_keys == ()


def test_specs_reject_missing_and_unknown_contract_sources() -> None:
    without_regions = {key: value for key, value in CONTRACTS.items() if key != "regions"}
    with pytest.raises(SilverLoadError, match="missing for sources"):
        build_table_specs(without_regions)
    with pytest.raises(SilverLoadError, match="no silver table mapping"):
        build_table_specs({**CONTRACTS, "mystery": CONTRACTS["regions"]})


@pytest.mark.parametrize("bad", ["users; DROP TABLE x", "1st", "", "a b", 'a"b', "table.name"])
def test_ident_rejects_unsafe_identifiers(bad: str) -> None:
    with pytest.raises(SilverLoadError, match="unsafe SQL identifier"):
        ident(bad)


def test_ident_quotes_valid_names() -> None:
    assert ident("sales") == '"sales"'
    assert ident("_x1") == '"_x1"'


def test_staging_ddl_creates_constraint_free_heap_tables() -> None:
    statements = render_staging_ddl(build_table_specs(CONTRACTS), "raw_staging")
    assert statements[0] == 'CREATE SCHEMA IF NOT EXISTS "raw_staging"'
    assert len(statements) == 1 + len(TABLE_LOAD_ORDER)
    body = "\n".join(statements)
    assert 'CREATE TABLE IF NOT EXISTS "raw_staging"."sales"' in body
    assert "PRIMARY KEY" not in body
    assert "FOREIGN KEY" not in body
    assert "NOT NULL" not in body


def test_silver_ddl_declares_keys_checks_and_partial_unique_indexes() -> None:
    statements = render_silver_ddl(build_table_specs(CONTRACTS), "silver")
    body = "\n".join(statements)
    assert statements[0] == 'CREATE SCHEMA IF NOT EXISTS "silver"'
    assert 'PRIMARY KEY ("sale_id")' in body
    assert 'FOREIGN KEY ("store_id") REFERENCES "silver"."stores" ("store_id")' in body
    assert "IF NOT EXISTS (SELECT 1 FROM pg_constraint" in body
    assert 'CONSTRAINT "ck_weather_dates" CHECK' in body
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "ux_weather_open_meteo_history"' in body
    assert 'CREATE UNIQUE INDEX IF NOT EXISTS "ux_weather_open_meteo_forecast"' in body
    assert 'WHERE "observation_date" IS NOT NULL' in body
    assert 'WHERE "forecast_date" IS NOT NULL' in body
    weather_line = next(
        line
        for line in body.splitlines()
        if 'CREATE TABLE IF NOT EXISTS "silver"."weather"' in line
    )
    assert "PRIMARY KEY" not in weather_line


def test_audit_ddl_builds_the_registry_with_a_success_guard() -> None:
    body = "\n".join(render_audit_ddl("audit"))
    assert 'CREATE TABLE IF NOT EXISTS "audit"."load_audit"' in body
    assert "load_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY" in body
    assert "CHECK (status IN ('success', 'failed', 'skipped'))" in body
    assert "ux_load_audit_success ON" in body
    assert "WHERE status = 'success'" in body
    assert "ix_load_audit_run ON" in body
    assert "ix_load_audit_target ON" in body


def test_render_schema_ddl_combines_all_three_schemas() -> None:
    specs = build_table_specs(CONTRACTS)
    statements = render_schema_ddl(
        specs, staging_schema="stg", silver_schema="slv", audit_schema="aud"
    )
    joined = "\n".join(statements)
    assert 'CREATE SCHEMA IF NOT EXISTS "stg"' in joined
    assert 'CREATE SCHEMA IF NOT EXISTS "slv"' in joined
    assert 'CREATE SCHEMA IF NOT EXISTS "aud"' in joined
    assert '"aud"."load_audit"' in joined
    expected = (
        len(render_staging_ddl(specs, "stg"))
        + len(render_silver_ddl(specs, "slv"))
        + len(render_audit_ddl("aud"))
    )
    assert len(statements) == expected


def test_conflict_clause_updates_every_non_key_column() -> None:
    clause = conflict_clause(build_table_specs(CONTRACTS)["sales"], "sales")
    assert clause.startswith('ON CONFLICT ("sale_id") DO UPDATE SET ')
    assert '"sale_id" = EXCLUDED."sale_id"' not in clause
    assert '"quantity" = EXCLUDED."quantity"' in clause
    assert '"batch_id" = EXCLUDED."batch_id"' in clause


def test_conflict_clause_uses_partial_predicate_for_weather() -> None:
    clause = conflict_clause(build_table_specs(CONTRACTS)["weather"], "open_meteo_history")
    assert clause.startswith(
        'ON CONFLICT ("region_id", "observation_date") '
        'WHERE "observation_date" IS NOT NULL DO UPDATE SET '
    )
    assert '"region_id" = EXCLUDED."region_id"' not in clause
    assert '"forecast_date" = EXCLUDED."forecast_date"' in clause


def test_conflict_clause_rejects_sources_the_table_does_not_absorb() -> None:
    with pytest.raises(SilverLoadError, match="no conflict keys"):
        conflict_clause(build_table_specs(CONTRACTS)["sales"], "regions")


def test_parse_marker_requires_source_batch_parts_and_quality() -> None:
    with pytest.raises(SilverLoadError, match="not a JSON object"):
        _parse_marker("k", ["list"])
    with pytest.raises(SilverLoadError, match="missing source/batch"):
        _parse_marker("k", {"source": "", "batch": "", "parts": [{"key": "p", "bytes": 1}]})
    with pytest.raises(SilverLoadError, match="has no parts"):
        _parse_marker("k", {"source": "s", "batch": "b", "parts": []})
    with pytest.raises(SilverLoadError, match="malformed part"):
        _parse_marker("k", {"source": "s", "batch": "b", "parts": [{"bytes": 1}]})
    with pytest.raises(SilverLoadError, match="missing quality"):
        _parse_marker("k", {"source": "s", "batch": "b", "parts": [{"key": "p", "bytes": 1}]})
    with pytest.raises(SilverLoadError, match="records_checked"):
        _parse_marker(
            "k",
            {
                "source": "s",
                "batch": "b",
                "parts": [{"key": "p", "bytes": 1}],
                "quality": {"records_checked": -1},
            },
        )


def test_parse_marker_accepts_the_bronze_marker_shape() -> None:
    batch = _parse_marker(
        "_promoted/sales/sales__abc.json",
        {
            "run_id": "run-1",
            "source": "sales",
            "batch": "sales__abc",
            "parts": [{"key": "bronze/sales/part.parquet", "bytes": 128}],
            "quality": {"status": "passed", "records_checked": 3},
            "promoted_at": "2024-06-15T00:00:00+00:00",
        },
    )
    assert batch.source == "sales"
    assert batch.batch_id == "sales__abc"
    assert batch.part_keys == ("bronze/sales/part.parquet",)
    assert batch.part_bytes == (128,)
    assert batch.records_checked == 3
    assert batch.quality_status == "passed"
    assert batch.marker_key == "_promoted/sales/sales__abc.json"


def test_plan_batches_flags_unmapped_sources_bad_quality_and_orders_tables() -> None:
    unmapped = PromotedBatch(
        source="mystery",
        batch_id="mystery__b",
        marker_key="k1",
        part_keys=("p",),
        part_bytes=(1,),
        records_checked=1,
        quality_status="passed",
        run_id="r",
        promoted_at="t",
    )
    records, pending = plan_batches([unmapped], [], None)
    assert [record.outcome for record in records] == ["failed"]
    assert "no silver table mapping" in (records[0].error or "")
    assert pending == []

    unpassed = replace(unmapped, source="regions", quality_status="failed")
    records, pending = plan_batches([unpassed], [], None)
    assert [record.outcome for record in records] == ["excluded"]
    assert pending == []

    regions = replace(unmapped, source="regions", batch_id="regions__b")
    sales = replace(unmapped, source="sales", batch_id="sales__b")
    records, pending = plan_batches([sales, regions], [], None)
    assert records == []
    assert [table for _batch, table in pending] == ["regions", "sales"]

    records, pending = plan_batches([sales, regions], [], ["sales"])
    assert [batch.source for batch, _table in pending] == ["sales"]


def test_dry_run_plans_promoted_batches_without_touching_postgresql(
    tmp_path: Path,
) -> None:
    store, settings = promoted_store(tmp_path)
    loader = loader_for(tmp_path, store)
    result = loader.run()

    assert result.ok
    assert result.dry_run is True
    assert result.counts == {
        "loaded": 0,
        "already_loaded": 0,
        "failed": 0,
        "excluded": 0,
        "planned": 5,
    }
    assert "reconciliation" not in result.manifest
    assert result.manifest["dry_run"] is True
    assert result.manifest["schemas"] == {
        "staging": "raw_staging",
        "silver": "silver",
        "audit": "audit",
    }
    tables = [record.target_table for record in result.records]
    assert tables == ["regions", "channels", "stores", "products", "sales"]


def test_dry_run_writes_a_local_manifest(tmp_path: Path) -> None:
    store, settings = promoted_store(tmp_path)
    result = loader_for(tmp_path, store).run()
    assert result.manifest_path is not None
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert payload["run_id"] == result.run_id
    assert payload["counts"]["planned"] == 5
    assert len(payload["records"]) == 5
    assert set(payload["timing"]) == {
        "schema_seconds",
        "read_seconds",
        "db_seconds",
        "reconcile_seconds",
        "total_seconds",
    }
    assert result.manifest_path.parent == tmp_path / "silver" / "manifests"


def test_source_filter_limits_planned_batches(tmp_path: Path) -> None:
    store, settings = promoted_store(tmp_path)
    result = loader_for(tmp_path, store).run(sources=["sales"])
    assert result.counts["planned"] == 1
    assert all(record.source == "sales" for record in result.records)


def test_unknown_source_is_a_usage_error(tmp_path: Path) -> None:
    store, settings = promoted_store(tmp_path)
    loader = loader_for(tmp_path, store)
    with pytest.raises(SilverLoadError, match="unknown source"):
        loader.run(sources=["mystery"])


def test_quality_failed_marker_is_excluded_without_reading_parts(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    store = InMemoryObjectStore()
    craft_marker(
        store,
        settings,
        {
            "run_id": "run-1",
            "source": "sales",
            "batch": "sales__broken",
            "parts": [{"key": "bronze/sales/missing.parquet", "bytes": 10}],
            "quality": {"status": "failed", "records_checked": 3},
            "promoted_at": "2024-06-15T00:00:00+00:00",
        },
        "sales/sales__broken.json",
    )
    result = loader_for(tmp_path, store).run()
    assert result.counts["excluded"] == 1
    assert result.counts["failed"] == 0
    assert result.ok
    assert "not promoted" in (result.records[0].error or "")


def test_malformed_markers_are_reported_as_failed(tmp_path: Path) -> None:
    settings = make_settings(tmp_path)
    store = InMemoryObjectStore()
    store.put_bytes(
        settings.bucket_bronze, f"{settings.promoted_prefix}/sales/bad.json", b"{not json"
    )
    store.put_bytes(
        settings.bucket_bronze,
        f"{settings.promoted_prefix}/regions/no-parts.json",
        json.dumps({"source": "regions", "batch": "regions__x"}).encode("utf-8"),
    )
    result = loader_for(tmp_path, store).run()
    assert len(result.failed) == 2
    assert all(
        (record.error or "").startswith("invalid promotion marker") for record in result.failed
    )
    assert not result.ok
    assert result.counts["planned"] == 0


def test_conninfo_for_builds_a_parseable_dsn(tmp_path: Path) -> None:
    from psycopg.conninfo import conninfo_to_dict

    settings = replace(
        silver_settings(tmp_path),
        postgres_host="db.internal",
        postgres_port=5433,
        postgres_db="warehouse",
        postgres_user="loader",
        postgres_password="secret",
        postgres_sslmode="require",
    )
    parsed = conninfo_to_dict(conninfo_for(settings))
    assert parsed["host"] == "db.internal"
    assert parsed["port"] == "5433"
    assert parsed["dbname"] == "warehouse"
    assert parsed["user"] == "loader"
    assert parsed["sslmode"] == "require"
    assert parsed["connect_timeout"] == "5"


def test_load_subcommand_is_registered_with_dry_run_and_force_flags() -> None:
    args = build_parser().parse_args(["load", "--source", "sales", "--dry-run", "--force"])
    assert args.command == "load"
    assert args.source == ["sales"]
    assert args.dry_run is True
    assert args.force is True


def test_raw_objects_survive_the_planning_run(tmp_path: Path) -> None:
    store, settings = promoted_store(tmp_path)
    raw_before = {
        key: store.get_bytes(settings.bucket_raw, key)
        for key in store.list_keys(settings.bucket_raw)
    }
    loader_for(tmp_path, store).run()
    raw_after = {
        key: store.get_bytes(settings.bucket_raw, key)
        for key in store.list_keys(settings.bucket_raw)
    }
    assert raw_after == raw_before
    assert f"raw/sales/sales__{DEFAULT_TAG}.csv" in raw_after


def test_planning_does_not_create_bronze_objects(tmp_path: Path) -> None:
    store, settings = promoted_store(tmp_path)
    before = set(store.list_keys(settings.bucket_bronze))
    loader_for(tmp_path, store).run()
    assert set(store.list_keys(settings.bucket_bronze)) == before


def test_silver_run_result_ok_follows_failed_records_and_reconciliation() -> None:
    failed = SilverRunResult(
        run_id="r",
        dry_run=True,
        records=(
            SilverRecord(
                run_id="r",
                source="sales",
                batch="sales__x",
                target_table="sales",
                outcome="failed",
                error="boom",
            ),
        ),
        manifest={"counts": {"failed": 1}, "reconciliation_balanced": True},
        manifest_path=None,
    )
    assert not failed.ok
    unbalanced = SilverRunResult(
        run_id="r",
        dry_run=False,
        records=(),
        manifest={"counts": {}, "reconciliation_balanced": False},
        manifest_path=None,
    )
    assert not unbalanced.ok
    balanced = SilverRunResult(
        run_id="r",
        dry_run=False,
        records=(),
        manifest={"counts": {}, "reconciliation_balanced": True},
        manifest_path=None,
    )
    assert balanced.ok
