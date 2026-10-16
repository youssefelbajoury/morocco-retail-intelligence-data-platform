"""Strict standardization of raw source rows into typed Bronze frames.

Bronze converts one raw object (CSV or JSON, already validated at ingest time)
into one typed :class:`pandas.DataFrame` that can be written as partitioned
Parquet.  The conversion is deliberately strict: cells are parsed from text,
an unparsable value never silently coerces (``"not-a-number"`` never becomes
``0``), and any row that violates the contract's type or nullability rules is
excluded from the Bronze frame and reported as a rejected row.

Partitioning follows the access pattern: only the large fact sources
(``sales``, ``purchases``, ``inventory``) are partitioned by
``year=/month=/`` on their date column; every other source is written as a
single unpartitioned object.  Each part carries provenance columns.
"""

from __future__ import annotations

import csv
import io
import json
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd

from mri.ingestion.contracts import Column, Contract
from mri.ingestion.exceptions import SourceFileError

PARTITION_COLUMNS: dict[str, str] = {
    "sales": "sale_date",
    "purchases": "purchase_date",
    "inventory": "snapshot_date",
}

MAX_ISSUES = 1000
MAX_REJECTED_ROWS = 100
MAX_CELL_VALUE = 200
_INT_PATTERN = r"[+-]?[0-9]+"
_BOOL_MAP: dict[str, bool] = {"true": True, "1": True, "false": False, "0": False}
_NULLABLE_DTYPES: dict[str, Any] = {
    "integer": "Int64",
    "number": "Float64",
    "boolean": "boolean",
}
_NUMPY_DTYPES: dict[str, Any] = {
    "integer": "int64",
    "number": "float64",
    "boolean": "bool",
}


@dataclass(frozen=True)
class ConversionIssue:
    """One cell that violated the contract during standardization."""

    row: int  # 1-based data-row number inside the source object
    column: str
    code: str
    value: str


@dataclass(frozen=True)
class StandardizedBatch:
    """The typed frame for one raw object plus everything that was excluded."""

    frame: pd.DataFrame
    rows_in: int
    rows_kept: int
    rows_rejected: int
    issue_counts: dict[str, int]
    issues: tuple[ConversionIssue, ...]
    rejected_rows: tuple[dict[str, str], ...]
    fatal: str | None = None


def resolve_encoding(contract: Contract) -> str:
    if contract.encoding.lower() in {"utf-8", "utf8"}:
        return "utf-8-sig"
    return contract.encoding


def parse_rows(data: bytes, contract: Contract, fmt: str) -> pd.DataFrame:
    """Decode raw object bytes into a frame of text (CSV) or text-or-None (JSON) cells."""
    if fmt not in contract.format_types:
        raise SourceFileError(
            f"format {fmt!r} is not allowed by contract {contract.id} "
            f"(allowed: {', '.join(contract.format_types)})"
        )
    try:
        text = data.decode(resolve_encoding(contract))
    except UnicodeDecodeError as exc:
        raise SourceFileError(
            f"cannot decode {contract.id} object using {contract.encoding}: {exc}"
        ) from exc
    if fmt == "csv":
        return _parse_csv(text, contract)
    return _parse_json(text, contract)


def _parse_csv(text: str, contract: Contract) -> pd.DataFrame:
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=contract.delimiter or ",")
    try:
        header = [item.strip() for item in next(reader)]
    except StopIteration:
        raise SourceFileError(f"{contract.id}: file has no header row") from None
    if not header or header == [""]:
        raise SourceFileError(f"{contract.id}: file has no header row")
    index: dict[str, int] = {}
    for position, name in enumerate(header):
        index[name] = position  # a later duplicate name wins, as in Phase-04 validation
    names = list(index)
    positions = list(index.values())
    columns: list[list[str]] = [[] for _ in names]
    for row in reader:
        width = len(row)
        for slot, position in enumerate(positions):
            columns[slot].append(row[position] if position < width else "")
    return pd.DataFrame({name: values for name, values in zip(names, columns, strict=True)})


def _parse_json(text: str, contract: Contract) -> pd.DataFrame:
    stripped = text.lstrip()
    if not stripped:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    if stripped[0] in "[{":
        data = json.loads(text)
        items = data if isinstance(data, list) else [data]
        for position, item in enumerate(items):
            if not isinstance(item, dict):
                raise SourceFileError(f"{contract.id}: array element {position} is not an object")
            rows.append(item)
    else:
        for line_number, line in enumerate(text.splitlines(), start=1):
            payload = line.strip()
            if not payload:
                continue
            try:
                item = json.loads(payload)
            except json.JSONDecodeError as exc:
                raise SourceFileError(
                    f"{contract.id}: invalid JSON on line {line_number}: {exc}"
                ) from exc
            if not isinstance(item, dict):
                raise SourceFileError(f"{contract.id}: line {line_number} is not a JSON object")
            rows.append(item)
    if not rows:
        return pd.DataFrame()
    normalized = [{name: _json_cell(value) for name, value in item.items()} for item in rows]
    return pd.DataFrame(normalized, dtype=object)


def _json_cell(value: Any) -> Any:
    """Flatten one JSON cell to text (or None) so both formats share one converter."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "True" if value else "False"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _is_null_cell(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return bool(pd.isna(value))


def _text(value: Any) -> str | None:
    if value is None or (not isinstance(value, str) and bool(pd.isna(value))):
        return None
    return value if isinstance(value, str) else str(value)


def standardize(raw: pd.DataFrame, contract: Contract) -> StandardizedBatch:
    """Convert an all-text frame into typed contract columns, excluding bad rows."""
    rows_in = len(raw)
    present = set(raw.columns)
    missing = [c.name for c in contract.columns if c.required and c.name not in present]
    if missing:
        return StandardizedBatch(
            frame=pd.DataFrame(),
            rows_in=rows_in,
            rows_kept=0,
            rows_rejected=0,
            issue_counts={},
            issues=(),
            rejected_rows=(),
            fatal=f"missing required column(s): {', '.join(missing)}",
        )

    keep = pd.Series(True, index=raw.index, dtype=bool)
    issues: list[ConversionIssue] = []
    issue_counts: dict[str, int] = {}
    typed: dict[str, pd.Series] = {}

    for column in contract.columns:
        if column.name not in present:
            continue  # optional absent column: omitted from the Bronze frame
        text = raw[column.name]
        null_mask = _null_mask(text)
        if not column.nullable:
            keep &= ~null_mask
            for position in null_mask[null_mask].index[: _remaining(issues)]:
                _record(
                    issues,
                    issue_counts,
                    ConversionIssue(
                        row=_row_number(position),
                        column=column.name,
                        code="null_violation",
                        value=_clip(_text(text.at[position])),
                    ),
                )
        converted, invalid = _convert(text, column, null_mask)
        if invalid.any():
            keep &= ~invalid
            for position in invalid[invalid].index[: _remaining(issues)]:
                _record(
                    issues,
                    issue_counts,
                    ConversionIssue(
                        row=_row_number(position),
                        column=column.name,
                        code=f"invalid_{column.type}",
                        value=_clip(_text(text.at[position])),
                    ),
                )
        typed[column.name] = converted

    kept = raw.loc[keep]
    result = kept.copy()
    for name, converted in typed.items():
        series = converted.loc[keep]
        column = contract.column_map()[name]
        if column.type in _NUMPY_DTYPES:
            if series.isna().any():
                series = series.astype(_NULLABLE_DTYPES[column.type])
            else:
                series = series.astype(_NUMPY_DTYPES[column.type])
        elif column.type == "date":
            series = series.astype("datetime64[ns]")
        result[name] = series
    order = [c.name for c in contract.columns if c.name in result.columns]
    extras = [name for name in raw.columns if name not in order]
    result = result[order + extras]

    rejected_index = raw.index[~keep]
    rejected_rows = tuple(
        _rejected_row(raw, position) for position in rejected_index[:MAX_REJECTED_ROWS]
    )
    rows_rejected = int((~keep).sum())
    return StandardizedBatch(
        frame=result,
        rows_in=rows_in,
        rows_kept=len(result),
        rows_rejected=rows_rejected,
        issue_counts=issue_counts,
        issues=tuple(issues),
        rejected_rows=rejected_rows,
        fatal=None,
    )


def _null_mask(text: pd.Series) -> pd.Series:
    return text.isna() | text.str.strip().eq("")


def _remaining(issues: list[ConversionIssue]) -> int:
    return max(0, MAX_ISSUES - len(issues))


def _record(issues: list[ConversionIssue], counts: dict[str, int], issue: ConversionIssue) -> None:
    counts[issue.code] = counts.get(issue.code, 0) + 1
    if len(issues) < MAX_ISSUES:
        issues.append(issue)


def _row_number(position: Any) -> int:
    return int(position) + 1


def _clip(value: str | None) -> str:
    if value is None:
        return ""
    return value if len(value) <= MAX_CELL_VALUE else value[:MAX_CELL_VALUE] + "..."


def _rejected_row(raw: pd.DataFrame, position: Any) -> dict[str, str]:
    row: dict[str, str] = {}
    for name in raw.columns:
        value = _text(raw.at[position, name])
        row[str(name)] = "" if value is None else value
    return row


def _convert(text: pd.Series, column: Column, null_mask: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Convert one text column; returns (converted, invalid-mask) over the full index."""
    non_null = ~null_mask
    if column.type == "string":
        # None (not NaN) keeps the Arrow null semantics for the Parquet writer.
        converted = text.where(non_null, other=None)  # type: ignore[call-overload]
        return converted, pd.Series(False, index=text.index)

    candidate = text.str.strip().where(non_null)
    if column.type == "integer":
        numeric = pd.to_numeric(candidate, errors="coerce")
        well_formed = candidate.str.fullmatch(_INT_PATTERN, na=False)
        invalid = non_null & (well_formed.eq(False) | numeric.isna() | numeric.abs().ge(2**63))
        return numeric, invalid
    if column.type == "number":
        numeric = pd.to_numeric(candidate, errors="coerce")
        invalid = non_null & (numeric.isna() | numeric.abs().eq(float("inf")))
        return numeric, invalid
    if column.type == "boolean":
        mapped = candidate.str.lower().map(_BOOL_MAP)
        invalid = non_null & mapped.isna()
        return mapped, invalid
    if column.type == "date":
        parsed = pd.to_datetime(candidate, format="%Y-%m-%d", errors="coerce")
        invalid = non_null & parsed.isna()
        return parsed.astype("datetime64[ns]"), invalid
    raise SourceFileError(f"unsupported contract type {column.type!r} for column {column.name!r}")


def part_filename(batch_id: str) -> str:
    """Deterministic, filesystem-safe part name derived from the batch id."""
    safe = re.sub(r"[^A-Za-z0-9._-]+", "-", batch_id).strip("-")
    return f"part-{safe or 'batch'[:150]}.parquet"[:180]


def build_parts(
    frame: pd.DataFrame,
    *,
    source: str,
    batch_id: str,
    prefix: str,
) -> list[tuple[str, bytes]]:
    """Serialize a standardized frame into one or more partitioned Parquet objects.

    Returns ``(object_key, parquet_bytes)`` pairs in deterministic (sorted
    partition) order.  Keys are content-derived through ``batch_id``, so the
    same raw object always maps to the same Bronze keys (idempotent writes).
    """
    name = part_filename(batch_id)
    partition = PARTITION_COLUMNS.get(source)
    if partition is None or partition not in frame.columns:
        return [(f"{prefix}/{source}/{name}", _serialize(frame))]
    partition_frame = frame[partition]
    year_key = partition_frame.dt.year
    month_key = partition_frame.dt.month
    pairs = frame.groupby([year_key, month_key], sort=True).size().index
    parts: list[tuple[str, bytes]] = []
    for year, month in pairs:
        mask = (year_key == year) & (month_key == month)
        chunk = frame[mask]
        if pd.isna(year) or pd.isna(month):
            path = f"{prefix}/{source}/year=__unknown__/month=__unknown__/{name}"
        else:
            path = f"{prefix}/{source}/year={int(year):04d}/month={int(month):02d}/{name}"
        parts.append((path, _serialize(chunk)))
    return parts


def _serialize(frame: pd.DataFrame) -> bytes:
    buffer = io.BytesIO()
    frame.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
    return buffer.getvalue()


def add_provenance(
    frame: pd.DataFrame,
    *,
    source: str,
    source_file: str,
    source_hash: str,
    run_id: str,
    ingestion_timestamp: str,
) -> pd.DataFrame:
    """Append the required provenance columns to a standardized frame."""
    out = frame.copy()
    out["source"] = source
    out["source_file"] = source_file
    out["source_hash"] = source_hash
    out["ingestion_run_id"] = run_id
    out["ingestion_timestamp"] = ingestion_timestamp
    return out


__all__ = [
    "MAX_ISSUES",
    "MAX_REJECTED_ROWS",
    "PARTITION_COLUMNS",
    "ConversionIssue",
    "StandardizedBatch",
    "add_provenance",
    "build_parts",
    "parse_rows",
    "part_filename",
    "standardize",
]
