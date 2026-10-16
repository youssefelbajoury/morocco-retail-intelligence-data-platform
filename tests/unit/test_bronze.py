"""Unit tests for Bronze parsing, strict standardization and partitioned parts.

Covers the six-phase fixture cases at schema level (valid, null violation,
invalid type, duplicate handled in the quality tests) plus partition keys,
provenance columns, determinism and the issue/recording caps.
"""

from __future__ import annotations

import io
import json

import pandas as pd
import pytest
from bronze_fixtures import (
    CONTRACTS,
    product_row,
    region_row,
    sales_row,
    standardized,
    store_row,
)

from mri.ingestion.bronze_schema import (
    MAX_ISSUES,
    MAX_REJECTED_ROWS,
    PARTITION_COLUMNS,
    add_provenance,
    build_parts,
    parse_rows,
    part_filename,
    standardize,
)
from mri.ingestion.exceptions import SourceFileError

SALES = "sales"
REGIONS = "regions"


def test_valid_sales_batch_keeps_every_row_with_contract_types() -> None:
    batch = standardized(SALES, [sales_row(), sales_row(sale_id="S000000002")])
    assert batch.fatal is None
    assert batch.rows_in == 2
    assert batch.rows_kept == 2
    assert batch.rows_rejected == 0
    assert batch.issue_counts == {}
    frame = batch.frame
    assert str(frame["sale_id"].dtype) == "object"
    assert str(frame["sale_date"].dtype) == "datetime64[ns]"
    assert str(frame["hour_of_day"].dtype) == "int64"
    assert str(frame["quantity"].dtype) == "int64"
    assert str(frame["unit_price_mad"].dtype) == "float64"
    assert str(frame["discount_pct"].dtype) == "float64"
    assert frame.loc[0, "sale_date"] == pd.Timestamp("2024-06-15")


def test_invalid_number_rejects_only_the_bad_row() -> None:
    batch = standardized(
        SALES,
        [
            sales_row(),
            sales_row(sale_id="S000000002", unit_price_mad="not-a-price"),
            sales_row(sale_id="S000000003"),
        ],
    )
    assert batch.rows_in == 3
    assert batch.rows_kept == 2
    assert batch.rows_rejected == 1
    assert batch.issue_counts == {"invalid_number": 1}
    issue = batch.issues[0]
    assert issue.row == 2
    assert issue.column == "unit_price_mad"
    assert issue.code == "invalid_number"
    assert issue.value == "not-a-price"
    assert list(batch.frame["sale_id"]) == ["S000000001", "S000000003"]
    assert batch.rejected_rows[0]["sale_id"] == "S000000002"
    assert batch.rejected_rows[0]["unit_price_mad"] == "not-a-price"


def test_null_in_non_nullable_column_rejects_the_row() -> None:
    batch = standardized(
        SALES,
        [sales_row(), sales_row(sale_id="S000000002", store_id="")],
    )
    assert batch.rows_kept == 1
    assert batch.rows_rejected == 1
    assert batch.issue_counts == {"null_violation": 1}
    assert batch.issues[0].column == "store_id"
    assert batch.issues[0].code == "null_violation"


def test_empty_nullable_cell_is_kept_as_null() -> None:
    batch = standardized(SALES, [sales_row(promotion_id=""), sales_row(promotion_id="PR01")])
    assert batch.rows_kept == 2
    assert batch.frame["promotion_id"].isna().tolist() == [True, False]


def test_missing_required_column_is_fatal() -> None:
    contract = CONTRACTS[REGIONS]
    header = [column.name for column in contract.columns if column.name != "demand_weight"]
    payload = (",".join(header) + "\n" + ",".join("x" for _ in header) + "\n").encode("utf-8")
    raw = parse_rows(payload, contract, "csv")
    batch = standardize(raw, contract)
    assert batch.fatal is not None
    assert "missing required column(s): demand_weight" in batch.fatal
    assert batch.rows_kept == 0
    assert batch.rows_rejected == 0


@pytest.mark.parametrize(
    ("source", "column", "value", "code"),
    [
        ("products", "unit_price_mad", "not-a-price", "invalid_number"),
        ("products", "launch_date", "June 15, 2019", "invalid_date"),
        ("sales", "hour_of_day", "12.5", "invalid_integer"),
        ("products", "is_active", "maybe", "invalid_boolean"),
    ],
)
def test_invalid_cell_values_rejects_the_row(
    source: str, column: str, value: str, code: str
) -> None:
    base = product_row() if source == "products" else sales_row()
    batch = standardized(source, [base, {**base, column: value}])
    assert batch.rows_in == 2
    assert batch.rows_kept == 1
    assert batch.rows_rejected == 1
    assert batch.issue_counts == {code: 1}
    assert batch.issues[0].column == column
    assert batch.issues[0].value == value


@pytest.mark.parametrize("value", ["true", "True", "TRUE", "1", "false", "False", "0"])
def test_boolean_spellings_are_accepted(value: str) -> None:
    batch = standardized("stores", [store_row(is_active=value)])
    assert batch.rows_kept == 1
    assert batch.issue_counts == {}


def test_integer_type_rejects_float_text() -> None:
    batch = standardized(SALES, [sales_row(quantity="12.0")])
    assert batch.rows_rejected == 1
    assert batch.issue_counts == {"invalid_integer": 1}
    kept = standardized(SALES, [sales_row(quantity="-5")])
    assert kept.rows_kept == 1


def test_extra_columns_are_preserved_after_contract_columns() -> None:
    contract = CONTRACTS[REGIONS]
    payload = b"region_id,region_name,demand_weight,import_batch\nR01,Alpha,0.5,nightly\n"
    raw = parse_rows(payload, contract, "csv")
    batch = standardize(raw, contract)
    assert list(batch.frame.columns)[-1] == "import_batch"
    assert batch.frame["import_batch"].tolist() == ["nightly"]


def test_ragged_row_is_padded_and_rejected_for_null_violation() -> None:
    payload = b"region_id,region_name,demand_weight\nR01,OnlyName\n"
    raw = parse_rows(payload, CONTRACTS[REGIONS], "csv")
    batch = standardize(raw, CONTRACTS[REGIONS])
    assert batch.rows_rejected == 1
    assert batch.issue_counts == {"null_violation": 1}
    assert batch.issues[0].column == "demand_weight"


def test_json_source_parses_and_standardizes() -> None:
    contract = CONTRACTS[REGIONS]
    payload = json.dumps(
        [{"region_id": "R01", "region_name": "Alpha", "demand_weight": 0.5}]
    ).encode("utf-8")
    raw = parse_rows(payload, contract, "json")
    batch = standardize(raw, contract)
    assert batch.rows_kept == 1
    assert str(batch.frame["demand_weight"].dtype) == "float64"


def test_disallowed_format_raises_source_file_error() -> None:
    with pytest.raises(SourceFileError, match="not allowed by contract"):
        parse_rows(b"<xml/>", CONTRACTS[REGIONS], "xml")


def test_issue_and_rejected_row_caps_are_enforced() -> None:
    rows = [sales_row(sale_id=f"S{i:08d}", unit_price_mad="bad") for i in range(1200)]
    batch = standardized(SALES, rows)
    assert batch.rows_in == 1200
    assert batch.rows_kept == 0
    assert batch.rows_rejected == 1200
    assert batch.issue_counts == {"invalid_number": 1000}  # capped at MAX_ISSUES
    assert len(batch.issues) == MAX_ISSUES
    assert len(batch.rejected_rows) == MAX_REJECTED_ROWS


def test_partitioned_sales_parts_follow_year_month_keys() -> None:
    batch = standardized(
        SALES,
        [sales_row(sale_date="2024-06-15"), sales_row(sale_id="S2", sale_date="2024-07-01")],
    )
    frame = add_provenance(
        batch.frame,
        source=SALES,
        source_file="raw/sales/sales__0123456789abcdef.csv",
        source_hash="deadbeef",
        run_id="run-1",
        ingestion_timestamp="2026-10-07T00:00:00Z",
    )
    parts = build_parts(frame, source=SALES, batch_id="sales__0123456789abcdef", prefix="bronze")
    assert [key for key, _blob in parts] == [
        "bronze/sales/year=2024/month=06/part-sales__0123456789abcdef.parquet",
        "bronze/sales/year=2024/month=07/part-sales__0123456789abcdef.parquet",
    ]
    first = pd.read_parquet(io.BytesIO(parts[0][1]))
    assert len(first) == 1
    assert first["sale_date"].dt.month.tolist() == [6]


def test_unpartitioned_source_writes_a_single_part() -> None:
    batch = standardized(REGIONS, [region_row()])
    parts = build_parts(batch.frame, source=REGIONS, batch_id="regions__abc", prefix="bronze")
    assert [key for key, _blob in parts] == ["bronze/regions/part-regions__abc.parquet"]


def test_partition_map_covers_exactly_the_fact_sources() -> None:
    assert set(PARTITION_COLUMNS) == {"sales", "purchases", "inventory"}


def test_part_building_is_deterministic() -> None:
    batch = standardized(
        SALES,
        [sales_row(sale_date="2024-06-15"), sales_row(sale_id="S2", sale_date="2024-06-20")],
    )
    first = build_parts(batch.frame, source=SALES, batch_id="sales__abc", prefix="bronze")
    second = build_parts(batch.frame, source=SALES, batch_id="sales__abc", prefix="bronze")
    assert first == second


def test_part_filename_is_filesystem_safe() -> None:
    assert part_filename("sales__abc") == "part-sales__abc.parquet"
    name = part_filename("a/b c\\d:e")
    assert "/" not in name
    assert "\\" not in name
    assert " " not in name
    assert name.endswith(".parquet")


def test_provenance_columns_are_appended_with_values() -> None:
    batch = standardized(REGIONS, [region_row()])
    frame = add_provenance(
        batch.frame,
        source=REGIONS,
        source_file="raw/regions/regions__0123456789abcdef.csv",
        source_hash="abc123",
        run_id="run-42",
        ingestion_timestamp="2026-10-07T12:00:00Z",
    )
    provenance = [
        "source",
        "source_file",
        "source_hash",
        "ingestion_run_id",
        "ingestion_timestamp",
    ]
    assert list(frame.columns)[-5:] == provenance
    assert frame["source"].tolist() == [REGIONS]
    assert frame["source_hash"].tolist() == ["abc123"]
    assert frame["ingestion_run_id"].tolist() == ["run-42"]
