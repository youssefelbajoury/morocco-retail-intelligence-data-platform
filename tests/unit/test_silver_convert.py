"""Unit tests for the Silver COPY value converters.

Every value crossing PostgreSQL text-mode ``COPY`` is produced here from
pandas/numpy cells according to the *target* column type: missing markers
become ``NULL``, fast dtype paths are verified against the generic fallback,
and values that cannot be represented fail loudly instead of being coerced.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import numpy as np
import pandas as pd
import pytest

from mri.ingestion.exceptions import SilverLoadError
from mri.silver.convert import convert_column, convert_value, frame_to_rows

ALL_PG_TYPES = ("TEXT", "BIGINT", "DOUBLE PRECISION", "BOOLEAN", "DATE", "TIMESTAMPTZ")


@pytest.mark.parametrize("pg_type", ALL_PG_TYPES)
def test_missing_markers_become_null_for_every_type(pg_type: str) -> None:
    assert convert_value(None, pg_type) is None
    assert convert_value(float("nan"), pg_type) is None
    assert convert_value(pd.NaT, pg_type) is None
    assert convert_value(pd.NA, pg_type) is None


def test_text_converts_scalars_to_strings() -> None:
    assert convert_value(42, "TEXT") == "42"
    assert convert_value(1.5, "TEXT") == "1.5"
    assert convert_value(True, "TEXT") == "True"


def test_bigint_unboxes_numbers_but_rejects_booleans() -> None:
    assert convert_value(42, "BIGINT") == 42
    assert convert_value(np.int64(7), "BIGINT") == 7
    assert convert_value(42.0, "BIGINT") == 42
    with pytest.raises(SilverLoadError, match="boolean"):
        convert_value(True, "BIGINT")


def test_double_unboxes_numbers_but_rejects_booleans() -> None:
    assert convert_value(1.5, "DOUBLE PRECISION") == 1.5
    assert convert_value(np.float64(2.25), "DOUBLE PRECISION") == 2.25
    with pytest.raises(SilverLoadError, match="boolean"):
        convert_value(False, "DOUBLE PRECISION")


def test_boolean_only_accepts_booleans() -> None:
    assert convert_value(True, "BOOLEAN") is True
    assert convert_value(np.bool_(False), "BOOLEAN") is False
    with pytest.raises(SilverLoadError, match="BOOLEAN"):
        convert_value("true", "BOOLEAN")
    with pytest.raises(SilverLoadError, match="BOOLEAN"):
        convert_value(1, "BOOLEAN")


def test_date_accepts_dates_datetimes_and_iso_strings() -> None:
    assert convert_value(date(2024, 6, 15), "DATE") == date(2024, 6, 15)
    assert convert_value(datetime(2024, 6, 15, 10, 30), "DATE") == date(2024, 6, 15)
    assert convert_value("2024-06-15", "DATE") == date(2024, 6, 15)
    assert convert_value("2024-06-15T10:30:00", "DATE") == date(2024, 6, 15)
    with pytest.raises(SilverLoadError, match="DATE"):
        convert_value(42, "DATE")


def test_timestamptz_normalizes_datetimes_to_utc() -> None:
    naive = convert_value(datetime(2024, 6, 15, 10, 30), "TIMESTAMPTZ")
    assert naive == datetime(2024, 6, 15, 10, 30, tzinfo=UTC)
    aware = datetime(2024, 6, 15, 10, 30, tzinfo=UTC)
    assert convert_value(aware, "TIMESTAMPTZ") is aware
    combined = convert_value(date(2024, 6, 15), "TIMESTAMPTZ")
    assert combined == datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
    assert convert_value("2024-06-15 10:30:00+00", "TIMESTAMPTZ") == "2024-06-15 10:30:00+00"
    with pytest.raises(SilverLoadError, match="TIMESTAMPTZ"):
        convert_value(42, "TIMESTAMPTZ")


def test_unknown_target_type_is_rejected() -> None:
    with pytest.raises(SilverLoadError, match="unsupported"):
        convert_value("x", "JSONB")
    with pytest.raises(SilverLoadError, match="unsupported"):
        convert_column(pd.Series(["x"]), "JSONB")
    with pytest.raises(SilverLoadError, match="no target PostgreSQL type"):
        list(frame_to_rows(pd.DataFrame({"a": [1]}), ["a", "b"], {"a": "BIGINT"}))


def test_integer_fast_path_returns_plain_python_ints() -> None:
    values = convert_column(pd.Series([1, 2, 3]), "BIGINT")
    assert values == [1, 2, 3]
    assert all(type(value) is int for value in values)


def test_boolean_fast_path_returns_plain_python_bools() -> None:
    values = convert_column(pd.Series([True, False]), "BOOLEAN")
    assert values == [True, False]
    assert all(type(value) is bool for value in values)


def test_float_fast_path_turns_nan_into_null() -> None:
    values = convert_column(pd.Series([1.5, float("nan"), -2.0]), "DOUBLE PRECISION")
    assert values == [1.5, None, -2.0]


def test_datetime_fast_path_converts_to_date_and_marks_nat_as_null() -> None:
    series = pd.Series([pd.Timestamp("2024-06-15"), pd.NaT])
    assert convert_column(series, "DATE") == [date(2024, 6, 15), None]


def test_datetime_fast_path_attaches_utc_to_timestamptz() -> None:
    series = pd.Series([pd.Timestamp("2024-06-15 10:30:00"), pd.NaT])
    values = convert_column(series, "TIMESTAMPTZ")
    assert values[0] == datetime(2024, 6, 15, 10, 30, tzinfo=UTC)
    assert values[1] is None


def test_object_column_mixes_nulls_and_converted_values() -> None:
    series = pd.Series([1, None, np.nan, pd.NA, 4], dtype=object)
    assert convert_column(series, "BIGINT") == [1, None, None, None, 4]


def test_text_column_passes_strings_through_unchanged() -> None:
    assert convert_column(pd.Series(["a", "b"]), "TEXT") == ["a", "b"]


def test_frame_to_rows_yields_tuples_in_the_requested_column_order() -> None:
    frame = pd.DataFrame({"name": ["a", "b"], "qty": [1, 2]})
    types = {"qty": "BIGINT", "name": "TEXT"}
    rows = list(frame_to_rows(frame, ["qty", "name"], types))
    assert rows == [(1, "a"), (2, "b")]
    assert all(len(row) == 2 for row in rows)
