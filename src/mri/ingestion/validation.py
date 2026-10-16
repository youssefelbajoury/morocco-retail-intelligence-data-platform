"""Contract-driven validation of source files.

Phase 04 validates: the file format is allowed by the contract, required
columns are present, and values in a bounded sample conform to their declared
types (with null-safety honouring the ``nullable`` flag). Uniqueness, ranges
and referential rules remain declared in the contract for the future Great
Expectations gate and are intentionally not executed here.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from mri.ingestion.contracts import Contract
from mri.ingestion.exceptions import SourceFileError

MAX_ISSUES = 100
DEFAULT_SAMPLE_ROWS = 1000


@dataclass(frozen=True)
class ValidationIssue:
    """A single reason a file failed validation."""

    code: str
    message: str
    column: str | None = None
    row: int | None = None


@dataclass(frozen=True)
class ValidationResult:
    """Outcome of validating one file against one contract."""

    ok: bool
    contract_id: str
    contract_version: str
    format: str
    row_count: int
    columns: tuple[str, ...]
    issues: tuple[ValidationIssue, ...]

    def counts_by_code(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for issue in self.issues:
            counts[issue.code] = counts.get(issue.code, 0) + 1
        return counts

    @property
    def error_summary(self) -> str:
        if not self.issues:
            return "ok"
        return "; ".join(f"{issue.code}: {issue.message}" for issue in self.issues[:5])


def _is_null(value: object) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _valid_string(value: object) -> bool:
    return isinstance(value, str)


def _valid_integer(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, float):
        return value.is_integer()
    if isinstance(value, str):
        text = value.strip()
        if text.startswith(("+", "-")):
            text = text[1:]
        return text.isdigit()
    return False


def _valid_number(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        try:
            float(value.strip())
        except ValueError:
            return False
        return True
    return False


def _valid_boolean(value: object) -> bool:
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return value in (0, 1)
    if isinstance(value, str):
        return value.strip().lower() in {"true", "false", "0", "1"}
    return False


def _valid_date(value: object) -> bool:
    if isinstance(value, date):
        return True
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return False
        try:
            date.fromisoformat(text)
        except ValueError:
            return False
        return True
    return False


_VALIDATORS = {
    "string": _valid_string,
    "integer": _valid_integer,
    "number": _valid_number,
    "boolean": _valid_boolean,
    "date": _valid_date,
}


def _resolve_encoding(contract: Contract) -> str:
    if contract.encoding.lower() in {"utf-8", "utf8"}:
        return "utf-8-sig"
    return contract.encoding


def _read_csv(path: Path, contract: Contract) -> tuple[list[str], list[list[str]], int]:
    encoding = _resolve_encoding(contract)
    delimiter = contract.delimiter or ","
    try:
        with path.open("r", encoding=encoding, newline="") as handle:
            reader = csv.reader(handle, delimiter=delimiter)
            try:
                header = [item.strip() for item in next(reader)]
            except StopIteration:
                return [], [], 0
            sample: list[list[str]] = []
            total = 0
            for row in reader:
                total += 1
                if len(sample) < DEFAULT_SAMPLE_ROWS:
                    sample.append(row)
    except UnicodeDecodeError as exc:
        raise SourceFileError(
            f"cannot decode {path.name} using {contract.encoding}: {exc}"
        ) from exc
    return header, sample, total


def _load_json_rows(path: Path, contract: Contract) -> list[dict[str, Any]]:
    encoding = _resolve_encoding(contract)
    try:
        text = path.read_text(encoding=encoding)
    except UnicodeDecodeError as exc:
        raise SourceFileError(
            f"cannot decode {path.name} using {contract.encoding}: {exc}"
        ) from exc
    stripped = text.lstrip()
    if not stripped:
        return []
    rows: list[dict[str, Any]] = []
    if stripped[0] in "[{":
        data = json.loads(text)
        items = data if isinstance(data, list) else [data]
        for index, item in enumerate(items):
            if not isinstance(item, dict):
                raise SourceFileError(f"{path.name}: array element {index} is not an object")
            rows.append(item)
        return rows
    for index, line in enumerate(text.splitlines(), start=1):
        payload = line.strip()
        if not payload:
            continue
        try:
            item = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise SourceFileError(f"{path.name}: invalid JSON on line {index}: {exc}") from exc
        if not isinstance(item, dict):
            raise SourceFileError(f"{path.name}: line {index} is not a JSON object")
        rows.append(item)
    return rows


def _column_order(rows: list[dict[str, Any]]) -> tuple[str, ...]:
    order: list[str] = []
    seen: set[str] = set()
    for row in rows[:DEFAULT_SAMPLE_ROWS] or rows[:1]:
        for key in row:
            if key not in seen:
                seen.add(key)
                order.append(key)
    return tuple(order)


def validate_file(
    path: Path,
    contract: Contract,
    fmt: str,
    *,
    sample_rows: int = DEFAULT_SAMPLE_ROWS,
) -> ValidationResult:
    """Validate a source file against its contract and return a structured result."""
    issues: list[ValidationIssue] = []

    if fmt not in contract.format_types:
        issues.append(
            ValidationIssue(
                code="FORMAT_NOT_ALLOWED",
                message=f"format {fmt!r} is not allowed by contract (allowed: "
                f"{', '.join(contract.format_types)})",
            )
        )
        return _finalise(contract, fmt, 0, (), issues)

    if fmt == "csv":
        header, sample, row_count = _read_csv(path, contract)
        if not header:
            issues.append(ValidationIssue("EMPTY_FILE", "file has no header row"))
            return _finalise(contract, fmt, 0, (), issues)
        columns = tuple(header)
        index = {name: position for position, name in enumerate(header)}
        _check_columns(contract, columns, issues)
        _check_rows_csv(contract, sample, index, issues)
        return _finalise(contract, fmt, row_count, columns, issues)

    rows = _load_json_rows(path, contract)
    columns = _column_order(rows)
    _check_columns(contract, columns, issues)
    _check_rows_json(contract, rows, issues)
    return _finalise(contract, fmt, len(rows), columns, issues)


def _check_columns(
    contract: Contract,
    columns: tuple[str, ...],
    issues: list[ValidationIssue],
) -> None:
    present = set(columns)
    for column in contract.columns:
        if column.required and column.name not in present:
            issues.append(
                ValidationIssue(
                    code="MISSING_COLUMN",
                    message=f"required column {column.name!r} is absent",
                    column=column.name,
                )
            )


def _check_value(column_type: str, value: object) -> bool:
    validator = _VALIDATORS[column_type]
    return validator(value)


def _check_rows_csv(
    contract: Contract,
    sample: list[list[str]],
    index: dict[str, int],
    issues: list[ValidationIssue],
) -> None:
    for row_number, row in enumerate(sample, start=1):
        if len(issues) >= MAX_ISSUES:
            return
        for column in contract.columns:
            position = index.get(column.name)
            if position is None:
                continue
            value: object = row[position] if position < len(row) else ""
            _check_cell(column.name, column.type, column.nullable, value, row_number, issues)


def _check_rows_json(
    contract: Contract,
    rows: list[dict[str, Any]],
    issues: list[ValidationIssue],
) -> None:
    for row_number, row in enumerate(rows[:DEFAULT_SAMPLE_ROWS], start=1):
        if len(issues) >= MAX_ISSUES:
            return
        for column in contract.columns:
            if column.name not in row:
                continue
            _check_cell(
                column.name,
                column.type,
                column.nullable,
                row[column.name],
                row_number,
                issues,
            )


def _check_cell(
    name: str,
    column_type: str,
    nullable: bool,
    value: object,
    row_number: int,
    issues: list[ValidationIssue],
) -> None:
    if _is_null(value):
        if not nullable:
            issues.append(
                ValidationIssue(
                    code="MISSING_VALUE",
                    message=f"null value in non-nullable column {name!r}",
                    column=name,
                    row=row_number,
                )
            )
        return
    if not _check_value(column_type, value):
        issues.append(
            ValidationIssue(
                code="TYPE_MISMATCH",
                message=f"value {value!r} is not a valid {column_type}",
                column=name,
                row=row_number,
            )
        )


def _finalise(
    contract: Contract,
    fmt: str,
    row_count: int,
    columns: tuple[str, ...],
    issues: list[ValidationIssue],
) -> ValidationResult:
    truncated = issues[:MAX_ISSUES]
    if len(issues) > MAX_ISSUES:
        extra = len(issues) - MAX_ISSUES
        issues = issues[:MAX_ISSUES]
        issues.append(
            ValidationIssue(
                code="TRUNCATED",
                message=f"{extra} additional issue(s) not shown",
            )
        )
    else:
        issues = truncated
    return ValidationResult(
        ok=not issues,
        contract_id=contract.id,
        contract_version=contract.version,
        format=fmt,
        row_count=row_count,
        columns=columns,
        issues=tuple(issues),
    )
