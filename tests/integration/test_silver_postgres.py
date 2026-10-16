"""Integration tests for the Silver layer against a live PostgreSQL instance.

Skipped automatically when PostgreSQL is unreachable. The whole suite shares
one session: Bronze promotes the deterministic ``tiny`` dataset once from an
in-memory object store, then the tests load it through the three-schema
pipeline and prove the load contract end to end — happy path, idempotence,
registry-drift detection and force repair, GX-gated exclusion, constraint
enforcement via direct SQL, and rollback plus audit of a batch that violates
a foreign key.

Every session runs in its own uniquely suffixed ``raw_staging_it*``,
``silver_it*`` and ``audit_it*`` schemas which are dropped again afterwards,
so the suite never touches the development schemas and can run repeatedly.
"""

from __future__ import annotations

import csv
import io
import json
from collections.abc import Sequence
from typing import Any

import pandas as pd
import psycopg
import pytest
from silver_fixtures import RAW_TAG, SilverEnv

from mri.ingestion.bronze import BronzePipeline
from mri.ingestion.manifest import now_iso
from mri.silver import SilverLoader, SilverRunResult, conninfo_for
from mri.silver.ddl import qualified

LOADABLE_TABLES = (
    "calendar",
    "channels",
    "customers",
    "inventory",
    "products",
    "promotions",
    "purchases",
    "regions",
    "sales",
    "stores",
)
PROVENANCE = {
    "source": "integration",
    "source_file": "integration.csv",
    "source_hash": "deadbeef",
    "batch_id": "integration",
    "ingestion_run_id": "integration",
    "ingestion_timestamp": "2024-06-15T00:00:00+00",
}
WEATHER_BASE = (
    "R01",
    "Casablanca-Settat",
    "Casablanca",
    "Africa/Casablanca",
    33.57,
    -7.59,
    "integration",
    "2024-06-15T00:00:00+00",
)
WEATHER_COLUMNS = (
    "region_id, region_name, city, timezone, latitude, longitude, data_type, "
    "retrieval_time, observation_date, source, source_file, source_hash, "
    "batch_id, ingestion_run_id, ingestion_timestamp"
)


def _run(
    env: SilverEnv, *, sources: Sequence[str] | None = None, force: bool = False
) -> SilverRunResult:
    loader = SilverLoader(env.store, env.settings, env.contracts)
    return loader.run(sources=sources, force=force)


def _rows(env: SilverEnv, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(conninfo_for(env.settings)) as conn:
        return conn.execute(sql, params).fetchall()


def _count(env: SilverEnv, table: str) -> int:
    rows = _rows(env, f"SELECT count(*) FROM {qualified(env.settings.silver_schema, table)}")
    return int(rows[0][0])


def _statuses(env: SilverEnv) -> dict[str, int]:
    audit = qualified(env.settings.audit_schema, "load_audit")
    rows = _rows(env, f"SELECT status, count(*) FROM {audit} GROUP BY status")
    return {str(status): int(count) for status, count in rows}


def _table_sql(env: SilverEnv, table: str) -> str:
    return qualified(env.settings.silver_schema, table)


def _audit_sql(env: SilverEnv) -> str:
    return qualified(env.settings.audit_schema, "load_audit")


def test_happy_load_fills_every_table_with_audit_and_balanced_reconciliation(
    silver_env: SilverEnv,
) -> None:
    result = _run(silver_env)

    assert result.ok
    assert len(result.records) == len(LOADABLE_TABLES)
    assert result.counts == {
        "loaded": 10,
        "already_loaded": 0,
        "failed": 0,
        "excluded": 0,
        "planned": 0,
    }
    for record in result.records:
        assert record.outcome == "loaded"
        assert record.rows_read == silver_env.expected[record.target_table]
        assert record.rows_inserted == silver_env.expected[record.target_table]
        assert record.error is None

    for table in LOADABLE_TABLES:
        assert _count(silver_env, table) == silver_env.expected[table]

    assert _statuses(silver_env) == {"success": 10}
    assert _rows(
        silver_env, f"SELECT count(*) FROM {_audit_sql(silver_env)} WHERE rows_inserted = 0"
    ) == [(0,)]
    duplicates = _rows(
        silver_env,
        f"SELECT target_table, batch_id FROM {_audit_sql(silver_env)} WHERE status = 'success' "
        "GROUP BY target_table, batch_id HAVING count(*) > 1",
    )
    assert duplicates == []

    reconciliation = result.manifest["reconciliation"]
    assert len(reconciliation) == len(LOADABLE_TABLES)
    assert {entry["target_table"] for entry in reconciliation} == set(LOADABLE_TABLES)
    for entry in reconciliation:
        assert entry["balanced"] is True
        assert entry["bronze_distinct_keys"] == silver_env.expected[entry["target_table"]]
        assert entry["silver_rows"] == silver_env.expected[entry["target_table"]]
    assert result.manifest["reconciliation_balanced"] is True
    assert result.manifest["tables"]["sales"]["rows_inserted"] == silver_env.expected["sales"]

    assert result.manifest_path is not None and result.manifest_path.is_file()
    provenance = _rows(
        silver_env,
        f"SELECT count(*) FROM {_table_sql(silver_env, 'sales')} "
        f"WHERE batch_id = 'sales__{RAW_TAG}' AND source = 'sales'",
    )
    assert provenance == [(silver_env.expected["sales"],)]


def test_second_run_skips_registered_batches_and_keeps_audit_stable(
    silver_env: SilverEnv,
) -> None:
    result = _run(silver_env)

    assert result.ok
    assert result.counts["already_loaded"] == 10
    assert result.counts["loaded"] == 0
    assert _statuses(silver_env) == {"success": 10, "skipped": 10}
    for table in LOADABLE_TABLES:
        assert _count(silver_env, table) == silver_env.expected[table]


def test_manual_row_deletion_is_detected_then_force_reloads_repair_it(
    silver_env: SilverEnv,
) -> None:
    with psycopg.connect(conninfo_for(silver_env.settings)) as conn:
        conn.execute(f"DELETE FROM {_table_sql(silver_env, 'sales')}")

    drift = _run(silver_env)
    assert not drift.ok
    assert drift.counts["already_loaded"] == 10
    sales_entry = next(
        entry for entry in drift.manifest["reconciliation"] if entry["target_table"] == "sales"
    )
    assert sales_entry["bronze_distinct_keys"] == silver_env.expected["sales"]
    assert sales_entry["silver_rows"] == 0
    assert sales_entry["balanced"] is False
    assert drift.manifest["reconciliation_balanced"] is False
    assert _count(silver_env, "sales") == 0

    repair = _run(silver_env, force=True)
    assert repair.ok
    assert repair.counts["loaded"] == 10
    assert repair.counts["already_loaded"] == 0
    assert _count(silver_env, "sales") == silver_env.expected["sales"]
    assert all(entry["balanced"] for entry in repair.manifest["reconciliation"])
    assert _statuses(silver_env) == {"success": 10, "skipped": 20}


def test_quality_failed_bronze_batch_is_never_loaded(silver_env: SilverEnv) -> None:
    bad_row = {
        "sale_id": "S000000901",
        "transaction_id": "T000000901",
        "sale_date": "2024-06-15",
        "hour_of_day": "12",
        "store_id": "ST01",
        "channel_id": "CH01",
        "customer_id": "CU0001",
        "product_id": "P000001",
        "promotion_id": "",
        "quantity": "-5",
        "unit_price_mad": "25.50",
        "discount_pct": "0.10",
        "net_amount_mad": "45.90",
        "cost_amount_mad": "30.00",
        "margin_mad": "15.90",
    }
    buffer = io.StringIO(newline="")
    header = [column.name for column in silver_env.contracts["sales"].columns]
    writer = csv.DictWriter(buffer, fieldnames=header, extrasaction="ignore", restval="")
    writer.writeheader()
    writer.writerow(bad_row)
    silver_env.store.put_bytes(
        silver_env.settings.bucket_raw,
        f"raw/sales/sales__{RAW_TAG}bad.csv",
        buffer.getvalue().encode("utf-8"),
    )

    bronze = BronzePipeline(silver_env.store, silver_env.settings, silver_env.contracts)
    bronze_result = bronze.run(sources=["sales"])
    bad_record = next(record for record in bronze_result.records if record.batch.endswith("bad"))
    assert bad_record.quality_status == "failed"
    assert bad_record.promoted is False

    sales_markers = silver_env.store.list_keys(
        silver_env.settings.bucket_bronze, f"{silver_env.settings.promoted_prefix}/sales/"
    )
    assert sales_markers == [f"{silver_env.settings.promoted_prefix}/sales/sales__{RAW_TAG}.json"]

    before = _count(silver_env, "sales")
    result = _run(silver_env, sources=["sales"])
    assert result.ok
    assert result.counts["already_loaded"] == 1
    assert result.counts["failed"] == 0
    assert _count(silver_env, "sales") == before
    assert _statuses(silver_env)["success"] == 10


def test_constraints_and_indexes_are_enforced_by_postgresql(silver_env: SilverEnv) -> None:
    provenance_values = tuple(PROVENANCE.values())

    with psycopg.connect(conninfo_for(silver_env.settings)) as conn:
        with pytest.raises(psycopg.errors.UniqueViolation):
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {_table_sql(silver_env, 'regions')} "
                    "(region_id, region_name, demand_weight, source, source_file, source_hash, "
                    "batch_id, ingestion_run_id, ingestion_timestamp) "
                    "VALUES ('R01', 'duplicate', 0.1, %s, %s, %s, %s, %s, %s)",
                    provenance_values,
                )

        with pytest.raises(psycopg.errors.NotNullViolation):
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {_table_sql(silver_env, 'weather')} "
                    f"({WEATHER_COLUMNS}) VALUES (%s, NULL, %s, %s, %s, %s, %s, %s, "
                    "'2030-01-02', %s, %s, %s, %s, %s, %s)",
                    (WEATHER_BASE[0], *WEATHER_BASE[2:], *provenance_values),
                )

        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {_table_sql(silver_env, 'weather')} "
                    f"({WEATHER_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
                    "'2030-01-03', %s, %s, %s, %s, %s, %s)",
                    ("RXX", *WEATHER_BASE[1:], *provenance_values),
                )

        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {_table_sql(silver_env, 'weather')} "
                    f"({WEATHER_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
                    "NULL, %s, %s, %s, %s, %s, %s)",
                    (*WEATHER_BASE, *provenance_values),
                )

        conn.execute(
            f"INSERT INTO {_table_sql(silver_env, 'weather')} "
            f"({WEATHER_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
            "'2030-01-01', %s, %s, %s, %s, %s, %s)",
            (*WEATHER_BASE, *provenance_values),
        )
        conn.execute(
            f"INSERT INTO {_table_sql(silver_env, 'weather')} "
            "(region_id, region_name, city, timezone, latitude, longitude, data_type, "
            "retrieval_time, forecast_date, source, source_file, source_hash, "
            "batch_id, ingestion_run_id, ingestion_timestamp) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, '2030-01-01', %s, %s, %s, %s, %s, %s)",
            (*WEATHER_BASE, *provenance_values),
        )
        with pytest.raises(psycopg.errors.UniqueViolation):
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {_table_sql(silver_env, 'weather')} "
                    f"({WEATHER_COLUMNS}) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, "
                    "'2030-01-01', %s, %s, %s, %s, %s, %s)",
                    (*WEATHER_BASE, *provenance_values),
                )

        with pytest.raises(psycopg.errors.CheckViolation):
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {_audit_sql(silver_env)} (run_id, batch_id, source, "
                    "target_table, started_at, status) "
                    "VALUES ('r', 'b', 's', 'regions', %s, 'weird')",
                    (now_iso(),),
                )
        registered = _rows(
            silver_env,
            f"SELECT batch_id FROM {_audit_sql(silver_env)} "
            "WHERE status = 'success' AND target_table = 'regions'",
        )
        assert len(registered) == 1
        with pytest.raises(psycopg.errors.UniqueViolation):
            with conn.transaction():
                conn.execute(
                    f"INSERT INTO {_audit_sql(silver_env)} (run_id, batch_id, source, "
                    "target_table, started_at, status) "
                    "VALUES ('r2', %s, 'regions', 'regions', %s, 'success')",
                    (str(registered[0][0]), now_iso()),
                )


def test_fk_violating_batch_rolls_back_and_is_audited_as_failed(
    silver_env: SilverEnv,
) -> None:
    bad = silver_env.tables["purchases"].head(5).copy()
    bad.loc[bad.index[0], "product_id"] = "P999999"
    bad["source"] = "purchases"
    bad["source_file"] = f"raw/purchases/purchases__{RAW_TAG}bad.csv"
    bad["source_hash"] = "deadbeef"
    bad["ingestion_run_id"] = "integration-badfk"
    bad["ingestion_timestamp"] = pd.Timestamp("2024-06-15 00:00:00", tz="UTC")
    buffer = io.BytesIO()
    bad.to_parquet(buffer, index=False)
    blob = buffer.getvalue()
    part_key = "purchases/year=2024/month=01/part-purchases__badfk.parquet"
    marker_key = f"{silver_env.settings.promoted_prefix}/purchases/purchases__badfk.json"
    silver_env.store.put_bytes(silver_env.settings.bucket_bronze, part_key, blob)
    silver_env.store.put_bytes(
        silver_env.settings.bucket_bronze,
        marker_key,
        json.dumps(
            {
                "run_id": "integration-badfk",
                "source": "purchases",
                "batch": "purchases__badfk",
                "source_hash": "deadbeef",
                "parts": [{"key": part_key, "bytes": len(blob)}],
                "quality": {
                    "status": "passed",
                    "expectations_total": 1,
                    "expectations_failed": 0,
                    "records_checked": len(bad),
                },
                "promoted_at": now_iso(),
            }
        ).encode("utf-8"),
    )

    result = _run(silver_env, sources=["purchases"])

    assert not result.ok
    failed = result.failed
    assert len(failed) == 1
    assert failed[0].batch == "purchases__badfk"
    assert "foreign key" in (failed[0].error or "").lower()
    assert result.counts["already_loaded"] == 1

    assert _count(silver_env, "purchases") == silver_env.expected["purchases"]
    rows = _rows(
        silver_env,
        f"SELECT status, rows_inserted, error_message FROM {_audit_sql(silver_env)} "
        "WHERE batch_id = 'purchases__badfk'",
    )
    assert len(rows) == 1
    status, rows_inserted, error_message = rows[0]
    assert status == "failed"
    assert rows_inserted == 0
    assert error_message and "foreign key" in error_message.lower()
    assert _statuses(silver_env) == {"success": 10, "skipped": 22, "failed": 1}
