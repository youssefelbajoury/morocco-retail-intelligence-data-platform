"""Convert standardized Bronze frames into COPY-ready Python values.

Every value sent through PostgreSQL text-mode ``COPY`` must be a plain Python
scalar (or ``None``) that psycopg can serialize for the declared column type.
The conversion is driven by the *target* PostgreSQL type coming from the table
spec (not by pandas dtype guessing): numpy/pandas scalars are unboxed, missing
markers (``None``/``NaN``/``NaT``/``pd.NA``) become ``NULL``, and values that
cannot be represented in the target type fail loudly instead of being coerced.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, date, datetime
from typing import Any

import numpy as np
import pandas as pd

from mri.ingestion.exceptions import SilverLoadError


def _is_missing(value: Any) -> bool:
    if value is None or value is pd.NaT or value is pd.NA:
        return True
    if isinstance(value, float) and value != value:
        return True
    if isinstance(value, datetime) and value != value:
        return True
    return False


def _to_text(value: Any) -> str:
    return str(value)


def _to_bigint(value: Any) -> int:
    if isinstance(value, (bool, np.bool_)):
        raise SilverLoadError(f"cannot convert boolean {value!r} to BIGINT")
    return int(value)


def _to_double(value: Any) -> float:
    if isinstance(value, (bool, np.bool_)):
        raise SilverLoadError(f"cannot convert boolean {value!r} to DOUBLE PRECISION")
    return float(value)


def _to_boolean(value: Any) -> bool:
    if isinstance(value, (bool, np.bool_)):
        return bool(value)
    raise SilverLoadError(f"cannot convert {value!r} to BOOLEAN")


def _to_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value[:10])
    raise SilverLoadError(f"cannot convert {value!r} to DATE")


def _to_timestamptz(value: Any) -> Any:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time(), tzinfo=UTC)
    if isinstance(value, str):
        return value
    raise SilverLoadError(f"cannot convert {value!r} to TIMESTAMPTZ")


_CONVERTERS: dict[str, Callable[[Any], Any]] = {
    "TEXT": _to_text,
    "BIGINT": _to_bigint,
    "DOUBLE PRECISION": _to_double,
    "BOOLEAN": _to_boolean,
    "DATE": _to_date,
    "TIMESTAMPTZ": _to_timestamptz,
}


def convert_value(value: Any, pg_type: str) -> Any:
    """Convert one Python value for the given target PostgreSQL type."""
    if _is_missing(value):
        return None
    converter = _CONVERTERS.get(pg_type)
    if converter is None:
        raise SilverLoadError(f"unsupported PostgreSQL column type {pg_type!r}")
    return converter(value)


def convert_column(series: pd.Series, pg_type: str) -> list[Any]:
    """Convert a whole pandas Series into a list of COPY-ready values."""
    converter = _CONVERTERS.get(pg_type)
    if converter is None:
        raise SilverLoadError(f"unsupported PostgreSQL column type {pg_type!r}")
    dtype = series.dtype
    if not pd.api.types.is_extension_array_dtype(dtype):
        if pg_type == "BIGINT" and pd.api.types.is_integer_dtype(dtype):
            return list(series.tolist())
        if pg_type == "BOOLEAN" and pd.api.types.is_bool_dtype(dtype):
            return list(series.tolist())
        if pg_type == "DOUBLE PRECISION" and pd.api.types.is_float_dtype(dtype):
            return [None if value != value else value for value in series.tolist()]
        if pd.api.types.is_datetime64_any_dtype(dtype):
            if pg_type == "DATE":
                return [
                    None if value is None or value is pd.NaT else value.date()
                    for value in series.tolist()
                ]
            if pg_type == "TIMESTAMPTZ":
                return [
                    None
                    if value is None or value is pd.NaT
                    else (value if value.tzinfo is not None else value.replace(tzinfo=UTC))
                    for value in series.tolist()
                ]
    if not series.isna().any():
        return [converter(value) for value in series.tolist()]
    return [None if _is_missing(value) else converter(value) for value in series.tolist()]


def frame_to_rows(
    frame: pd.DataFrame,
    columns: Sequence[str],
    types: Mapping[str, str],
) -> Iterator[tuple[Any, ...]]:
    """Yield one COPY-ready tuple per row, ordered like ``columns``."""
    converted: list[list[Any]] = []
    for column in columns:
        if column not in types:
            raise SilverLoadError(f"no target PostgreSQL type for column {column!r}")
        converted.append(convert_column(frame[column], types[column]))
    yield from zip(*converted, strict=True)


__all__ = ["convert_column", "convert_value", "frame_to_rows"]
