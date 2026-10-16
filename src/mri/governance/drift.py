"""Contract drift detection.

The versioned YAML contracts under ``data_contracts/`` are the single source of
truth for what every source must look like.  The runtime implements those rules
in three places:

* :mod:`mri.ingestion.quality` — the Great Expectations bronze gate that turns
  each contract's ``quality.declared`` lines, plus generic volume/not-null/type/
  key-uniqueness expectations, into a runnable suite;
* :mod:`mri.ingestion.bronze_schema` — standardization driven by the contract
  columns and the hard-coded ``PARTITION_COLUMNS``;
* :mod:`mri.silver.ddl` — the ``SOURCE_TABLES`` source-to-table map, the
  ``PG_TYPES`` renderability map and the appended ``PROVENANCE_COLUMNS``.

Whenever an engineer edits one of those places without the others, the
contracts and the implementation quietly diverge.  This module detects that
drift and reports it as a machine-readable issue list:

1. **declared but not implemented** — a ``quality.declared`` rule has no runtime
   expectation behind it (the contract promises something nobody enforces);
2. **implemented but not declared** — an expectation/reference/unique-key check
   exists that no contract line declares (the runtime is stricter than the
   published contract);
3. **source mapping missing** — a contract id is absent from the Silver source
   map (no ``raw_staging``/``silver`` table, or a dangling map entry without a
   contract);
4. **contract version mismatch** — non-semver, an unsupported major version, or
   the same contract id declared by two files;
5. **unsupported column** — a contract column the runtime cannot represent
   (a reserved provenance name, an unrenderable type, or a dropped Bronze
   partition column).

To avoid false positives the detector understands the documented
abstractions:

* declared uniqueness/not-null lines are enforced by the **generic** primary-key
  and type expectations (they can legitimately be absent from the declared
  expectations, so the detector checks them against the contract's own primary
  key and nullability instead);
* natural-language grammar is tolerated: ``is unique and not null``,
  ``are ... unique``, ``composite key (...) is unique``, ``is between`` and
  compound ``X >= a and Y >= b`` lines all map onto the same canonical rules;
  regular expressions are compared after normalising ``\\d`` to ``[0-9]``;
* referential ``X exists in Y`` rules are implemented through the shared
  ``REFERENCE_COLUMNS`` table, so drift there is checked statically;
* bronze-only sources — external enrichment data intentionally kept out of the
  Silver map — are declared through :data:`BRONZE_ONLY_SOURCES` and only warn;
* a declared line the parser does not understand is a warning, never a failure.

The :func:`run_drift_report` entry point is exercised by
``tests/unit/test_contract_drift.py`` on the real contract directory, so any of
the five drift classes breaks CI the moment it is introduced.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from mri.ingestion.bronze_schema import PARTITION_COLUMNS
from mri.ingestion.contracts import (
    SUPPORTED_CONTRACT_MAJOR,
    Contract,
    default_contracts_dir,
    load_contract,
    parse_version,
)
from mri.ingestion.exceptions import ContractError
from mri.ingestion.quality import REFERENCE_COLUMNS, build_specs
from mri.silver.ddl import PG_TYPES, PROVENANCE_COLUMNS, SOURCE_TABLES

# ---------------------------------------------------------------------------
# Published constants and data types
# ---------------------------------------------------------------------------

DRIFT_KINDS: tuple[str, ...] = (
    "declared_not_implemented",
    "implemented_not_declared",
    "source_mapping_missing",
    "source_mapping_orphan",
    "version_mismatch",
    "unsupported_column",
    "contract_load_error",
)

#: External enrichment sources intentionally absent from the Silver source map
#: (documented exception; currently none — every source lands in Silver).
BRONZE_ONLY_SOURCES: frozenset[str] = frozenset()

_RESERVED_COLUMNS: frozenset[str] = frozenset(column.name for column in PROVENANCE_COLUMNS)

_EXPECTED_KINDS: dict[str, str] = {
    "expect_column_values_to_be_between": "between",
    "expect_column_pair_values_a_to_be_greater_than_b": "pair",
    "expect_column_values_to_be_in_set": "set",
    "expect_column_values_to_match_regex": "regex",
}

_SKELETON_DTYPES: dict[str, str] = {
    "string": "object",
    "integer": "int64",
    "number": "float64",
    "boolean": "bool",
    "date": "datetime64[ns]",
}


@dataclass(frozen=True)
class DriftIssue:
    """One detected drift (or an acknowledged, documented exception warning)."""

    source: str
    kind: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.kind}] {self.source}: {self.detail}"


@dataclass(frozen=True)
class DriftReport:
    """The outcome of probing one contract directory."""

    issues: tuple[DriftIssue, ...]
    warnings: tuple[DriftIssue, ...]
    sources_checked: int
    declared_rules: int
    implemented_rules: int

    @property
    def passed(self) -> bool:
        return not self.issues


@dataclass(frozen=True)
class DeclaredRule:
    """One canonical rule parsed from a contract ``quality.declared`` line."""

    kind: str
    subjects: tuple[str, ...]
    payload: Mapping[str, Any]
    line: str


# ---------------------------------------------------------------------------
# Declared-rule classification (contract side, tolerant of grammar variants)
# ---------------------------------------------------------------------------

_UNIQUE_RE = re.compile(
    r"^(?:composite key\s*\((?P<composite>[^)]+)\)|(?P<plain>.+?))\s+"
    r"(?:is|are)\s+unique(?:\s+and\s+not\s+null)?$"
)
_REFERENTIAL_RE = re.compile(
    r"^(?P<col>[A-Za-z_][A-Za-z0-9_]*)\s+exists in\s+"
    r"(?P<ref>[A-Za-z_][A-Za-z0-9_]*)$"
)
_REGEX_RE = re.compile(r"^(?P<col>[A-Za-z_][A-Za-z0-9_]*)\s+matches\s+(?P<pattern>.+)$")
_SET_RE = re.compile(r"^(?P<col>[A-Za-z_][A-Za-z0-9_]*)\s+(?:is\s+)?in\s+\{(?P<values>[^}]+)\}$")
_BETWEEN_RE = re.compile(
    r"^(?P<col>[A-Za-z_][A-Za-z0-9_]*)\s+(?:is\s+)?between\s+"
    r"(?P<lo>.+?)\s+and\s+(?P<hi>.+)$"
)
_SEGMENT_RE = re.compile(r"^(?P<col>[A-Za-z_][A-Za-z0-9_]*)\s*(?P<op><=|>=|>|<)\s*(?P<val>.+)$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_INT_RE = re.compile(r"^-?\d+$")
_FLOAT_RE = re.compile(r"^-?\d+\.\d+$")


def _parse_value(token: str) -> Any:
    """A literal bound/value; non-literals stay strings so pair rules win."""
    token = token.strip()
    if _DATE_RE.fullmatch(token):
        try:
            return date.fromisoformat(token)
        except ValueError:
            return token
    if _INT_RE.fullmatch(token):
        try:
            return int(token)
        except ValueError:
            return token
    if _FLOAT_RE.fullmatch(token):
        try:
            return float(token)
        except ValueError:
            return token
    return token


def _key_columns(text: str) -> tuple[str, ...]:
    return tuple(
        part.strip() for part in re.split(r",|(?:^|\s+)and(?:\s+|$)", text) if part.strip()
    )


def _classify_line(line: str) -> list[DeclaredRule]:
    """Split one declared line into zero or more canonical rules."""
    text = line.strip()
    unique = _UNIQUE_RE.match(text)
    if unique:
        raw = unique.group("composite") or unique.group("plain")
        keys = _key_columns(raw)
        valid = all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key) for key in keys)
        if keys and valid:
            return [
                DeclaredRule(
                    "uniqueness",
                    keys,
                    {"not_null": "not null" in text},
                    line,
                )
            ]
        return []
    referential = _REFERENTIAL_RE.match(text)
    if referential:
        return [
            DeclaredRule(
                "referential",
                (referential.group("col"), referential.group("ref")),
                {},
                line,
            )
        ]
    regex_rule = _REGEX_RE.match(text)
    if regex_rule:
        return [
            DeclaredRule(
                "regex",
                (regex_rule.group("col"),),
                {"pattern": regex_rule.group("pattern")},
                line,
            )
        ]
    set_rule = _SET_RE.match(text)
    if set_rule:
        values = frozenset(
            part.strip() for part in set_rule.group("values").split(",") if part.strip()
        )
        if values:
            return [
                DeclaredRule(
                    "set",
                    (set_rule.group("col"),),
                    {"values": values},
                    line,
                )
            ]
        return []
    between = _BETWEEN_RE.match(text)
    if between:
        lo = _parse_value(between.group("lo"))
        hi = _parse_value(between.group("hi"))
        if not isinstance(lo, str) and not isinstance(hi, str):
            return [
                DeclaredRule(
                    "between",
                    (between.group("col"),),
                    {"min": lo, "max": hi, "strict_min": False, "strict_max": False},
                    line,
                )
            ]
        return []
    rules: list[DeclaredRule] = []
    valid = True
    for segment in text.split(" and "):
        match = _SEGMENT_RE.match(segment.strip())
        if match is None:
            valid = False
            break
        op, col, raw = match.group("op"), match.group("col"), match.group("val").strip()
        value = _parse_value(raw)
        if isinstance(value, str):
            if op in {">=", ">"} and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
                rules.append(
                    DeclaredRule(
                        "pair",
                        (col, value),
                        {"or_equal": op == ">="},
                        line,
                    )
                )
            elif op in {"<=", "<"} and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
                rules.append(
                    DeclaredRule(
                        "pair",
                        (value, col),
                        {"or_equal": op == "<="},
                        line,
                    )
                )
            else:
                valid = False
                break
        else:
            if op in {">", ">="}:
                rules.append(
                    DeclaredRule(
                        "between",
                        (col,),
                        {
                            "min": value,
                            "max": None,
                            "strict_min": op == ">",
                            "strict_max": False,
                        },
                        line,
                    )
                )
            elif op in {"<", "<="}:
                rules.append(
                    DeclaredRule(
                        "between",
                        (col,),
                        {
                            "min": None,
                            "max": value,
                            "strict_min": False,
                            "strict_max": op == "<",
                        },
                        line,
                    )
                )
            else:
                valid = False
                break
    return rules if valid else []


def classifiable_declared(contract: Contract) -> tuple[list[DeclaredRule], list[str]]:
    """Return the canonical rules and the unparsable ``quality.declared`` lines."""
    declared = (contract.raw.get("quality") or {}).get("declared") or []
    rules: list[DeclaredRule] = []
    unparsed: list[str] = []
    for line in declared:
        parsed = _classify_line(str(line))
        if parsed:
            rules.extend(parsed)
        else:
            unparsed.append(str(line))
    return rules, unparsed


# ---------------------------------------------------------------------------
# Implemented-rule introspection (runtime side, from the real quality gate)
# ---------------------------------------------------------------------------


def _skeleton_frame(contract: Contract) -> pd.DataFrame:
    dtypes = {
        column.name: _SKELETON_DTYPES.get(column.type, "object") for column in contract.columns
    }
    return pd.DataFrame({name: pd.Series(dtype=dtype) for name, dtype in dtypes.items()})


def _norm_bound(value: Any) -> Any:
    if isinstance(value, datetime) and value.time() == time.min and value.tzinfo is None:
        return value.date()
    return value


def _normalize_pattern(value: str) -> str:
    return value.replace(r"\d", "[0-9]").replace(r"\w", "[A-Za-z0-9_]")


def _implemented_rules(contract: Contract) -> list[DeclaredRule]:
    """Introspect :func:`build_specs` and keep only the declared expectations."""
    frame = _skeleton_frame(contract)
    specs = build_specs(contract, frame, {}, [])
    rules: list[DeclaredRule] = []
    for spec in specs:
        expectation = spec.expectation
        expectation_type = str(expectation.expectation_type)
        kind = _EXPECTED_KINDS.get(expectation_type)
        if kind is None:
            continue
        kwargs: Mapping[str, Any] = expectation.dict()
        if kind == "between":
            rules.append(
                DeclaredRule(
                    "between",
                    (str(kwargs.get("column", "")),),
                    {
                        "min": _norm_bound(kwargs.get("min_value")),
                        "max": _norm_bound(kwargs.get("max_value")),
                        "strict_min": bool(kwargs.get("strict_min", False)),
                        "strict_max": bool(kwargs.get("strict_max", False)),
                    },
                    spec.declared,
                )
            )
        elif kind == "pair":
            rules.append(
                DeclaredRule(
                    "pair",
                    (
                        str(kwargs.get("column_A", "")),
                        str(kwargs.get("column_B", "")),
                    ),
                    {"or_equal": bool(kwargs.get("or_equal", True))},
                    spec.declared,
                )
            )
        elif kind == "set":
            values = kwargs.get("value_set") or []
            rules.append(
                DeclaredRule(
                    "set",
                    (str(kwargs.get("column", "")),),
                    {"values": frozenset(str(value) for value in values)},
                    spec.declared,
                )
            )
        else:  # regex
            rules.append(
                DeclaredRule(
                    "regex",
                    (str(kwargs.get("column", "")),),
                    {"pattern": str(kwargs.get("regex", ""))},
                    spec.declared,
                )
            )
    return rules


# ---------------------------------------------------------------------------
# Rule matching
# ---------------------------------------------------------------------------


def _same_payload(declared: Mapping[str, Any], implemented: Mapping[str, Any]) -> bool:
    if declared.keys() != implemented.keys():
        return False
    for key in declared:
        if key == "values":
            if frozenset(declared[key]) != frozenset(implemented[key]):
                return False
        elif key == "pattern":
            if _normalize_pattern(str(declared[key])) != _normalize_pattern(str(implemented[key])):
                return False
        elif key in {"min", "max"}:
            if _norm_bound(declared[key]) != _norm_bound(implemented[key]):
                return False
        elif declared[key] != implemented[key]:
            return False
    return True


def _matching(rule: DeclaredRule, implemented: list[DeclaredRule]) -> DeclaredRule | None:
    for candidate in implemented:
        if candidate.kind != rule.kind:
            continue
        if candidate.subjects != rule.subjects:
            continue
        if _same_payload(rule.payload, candidate.payload):
            return candidate
    return None


# ---------------------------------------------------------------------------
# Per-source checks
# ---------------------------------------------------------------------------


def _direction_pass(
    contract: Contract,
    declared: list[DeclaredRule],
    implemented: list[DeclaredRule],
    references: Mapping[str, tuple[tuple[str, str], ...]],
    issues: list[DriftIssue],
) -> None:
    source = contract.id
    reference_entries = {(reference, column) for reference, column in references.get(source, ())}
    for rule in declared:
        if rule.kind == "uniqueness":
            pk = set(contract.primary_key)
            if set(rule.subjects) != pk:
                issues.append(
                    DriftIssue(
                        source,
                        "declared_not_implemented",
                        f"uniqueness declared for {rule.subjects} but the contract "
                        f"primary key is {sorted(pk)} (generic uniqueness only enforces "
                        f"the key)",
                    )
                )
            if rule.payload.get("not_null"):
                nullable = [
                    name
                    for name in rule.subjects
                    if name not in contract.column_map() or contract.column_map()[name].nullable
                ]
                if nullable:
                    issues.append(
                        DriftIssue(
                            source,
                            "declared_not_implemented",
                            f"declared 'not null' on {rule.subjects} but {nullable} "
                            f"is/are nullable (generic not-null follows nullability)",
                        )
                    )
        elif rule.kind == "referential":
            column, reference = rule.subjects
            if (reference, column) not in reference_entries:
                issues.append(
                    DriftIssue(
                        source,
                        "declared_not_implemented",
                        f"{column} exists in {reference} has no REFERENCE_COLUMNS entry",
                    )
                )
        else:
            if _matching(rule, implemented) is None:
                issues.append(
                    DriftIssue(
                        source,
                        "declared_not_implemented",
                        f"declared rule is not implemented: '{rule.line}'",
                    )
                )
    for rule in implemented:
        if _matching(rule, declared) is None:
            issues.append(
                DriftIssue(
                    source,
                    "implemented_not_declared",
                    f"implemented rule has no declared counterpart: '{rule.line}'",
                )
            )
    for reference, column in reference_entries:
        rule = DeclaredRule("referential", (column, reference), {}, "")
        if _matching(rule, declared) is None:
            issues.append(
                DriftIssue(
                    source,
                    "implemented_not_declared",
                    f"'{column} exists in {reference}' is enforced but not declared",
                )
            )


def _check_source_map(
    contract: Contract,
    source_tables: Mapping[str, str],
    issues: list[DriftIssue],
    warnings: list[DriftIssue],
) -> None:
    source = contract.id
    if source in source_tables:
        return
    if source in BRONZE_ONLY_SOURCES:
        warnings.append(
            DriftIssue(
                source,
                "source_mapping_missing",
                "bronze-only source intentionally absent from the Silver source map "
                "(documented exception)",
            )
        )
    else:
        issues.append(
            DriftIssue(
                source,
                "source_mapping_missing",
                f"contract has no Silver operation/table mapping in SOURCE_TABLES ({source} -> ?)",
            )
        )


def _check_columns(
    contract: Contract,
    partition_columns: Mapping[str, str],
    issues: list[DriftIssue],
) -> None:
    source = contract.id
    reserved = sorted(set(contract.column_names) & _RESERVED_COLUMNS)
    if reserved:
        issues.append(
            DriftIssue(
                source,
                "unsupported_column",
                f"column(s) {reserved} collide with the Silver provenance columns "
                f"(source, source_file, source_hash, batch_id, ingestion_run_id, "
                f"ingestion_timestamp)",
            )
        )
    unrenderable = sorted(
        name for column in contract.columns if column.type not in PG_TYPES for name in [column.name]
    )
    if unrenderable:
        issues.append(
            DriftIssue(
                source,
                "unsupported_column",
                f"columns with a type the Silver layer cannot render: {unrenderable} "
                f"(PG_TYPES={sorted(PG_TYPES)})",
            )
        )
    partition = partition_columns.get(source)
    if partition and partition not in contract.column_names:
        issues.append(
            DriftIssue(
                source,
                "unsupported_column",
                f"Bronze partition column {partition!r} is missing from the contract columns",
            )
        )


# ---------------------------------------------------------------------------
# Directory-level checks
# ---------------------------------------------------------------------------


def _check_versions(
    files: list[Path],
    issues: list[DriftIssue],
    loaded_ids: set[str],
) -> list[tuple[Path, Any]]:
    """Light-weight id/version pass that survives unloadable contracts."""
    documents: list[tuple[Path, Any]] = []
    owners: dict[str, str] = {}
    for path in files:
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            issues.append(DriftIssue(path.stem, "contract_load_error", f"cannot read YAML: {exc}"))
            continue
        documents.append((path, data))
        if not isinstance(data, Mapping):
            issues.append(
                DriftIssue(path.stem, "contract_load_error", "YAML root is not a mapping")
            )
            continue
        identifier = str(data.get("id", "")).strip()
        version = str(data.get("version", "")).strip()
        if not identifier:
            issues.append(DriftIssue(path.stem, "contract_load_error", "file declares no id"))
            continue
        if identifier in owners:
            issues.append(
                DriftIssue(
                    identifier,
                    "version_mismatch",
                    f"contract id declared by two files: {owners[identifier]} and {path}",
                )
            )
        else:
            owners[identifier] = str(path)
        try:
            major = parse_version(version)[0]
        except ContractError as exc:
            issues.append(DriftIssue(identifier, "version_mismatch", str(exc)))
            continue
        if major != SUPPORTED_CONTRACT_MAJOR:
            issues.append(
                DriftIssue(
                    identifier,
                    "version_mismatch",
                    f"version {version!r} has unsupported major {major} (expected "
                    f"{SUPPORTED_CONTRACT_MAJOR})",
                )
            )
        loaded_ids.add(identifier)
    return documents


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_drift_report(
    contracts_dir: Path | None = None,
    *,
    source_tables: Mapping[str, str] | None = None,
    reference_columns: Mapping[str, tuple[tuple[str, str], ...]] | None = None,
    partition_columns: Mapping[str, str] | None = None,
) -> DriftReport:
    """Probe a contracts directory and report contract↔implementation drift."""
    base = contracts_dir or default_contracts_dir()
    if not base.is_dir():
        raise ContractError(f"contracts directory not found: {base}")
    files = sorted(base.glob("*.yml"))
    issues: list[DriftIssue] = []
    warnings: list[DriftIssue] = []
    loaded_ids: set[str] = set()
    documents = _check_versions(files, issues, loaded_ids)

    source_map = SOURCE_TABLES if source_tables is None else source_tables
    references = REFERENCE_COLUMNS if reference_columns is None else reference_columns
    partition_map = PARTITION_COLUMNS if partition_columns is None else partition_columns

    declared_total = 0
    implemented_total = 0
    sources_checked = 0
    for path, _data in documents:
        try:
            contract = load_contract(path, base)
        except ContractError as exc:
            issues.append(DriftIssue(path.stem, "contract_load_error", str(exc)))
            continue
        sources_checked += 1
        declared, unparsed = classifiable_declared(contract)
        declared_total += len(declared)
        for line in unparsed:
            warnings.append(
                DriftIssue(
                    contract.id,
                    "declared_not_implemented",
                    f"declared line could not be classified: '{line}' (warning only)",
                )
            )
        implemented = _implemented_rules(contract)
        implemented_total += len(implemented)
        _direction_pass(contract, declared, implemented, references, issues)
        _check_source_map(contract, source_map, issues, warnings)
        _check_columns(contract, partition_map, issues)

    for source in sorted(source_map):
        if source not in loaded_ids:
            issues.append(
                DriftIssue(
                    source,
                    "source_mapping_orphan",
                    f"SOURCE_TABLES maps {source!r} but no contract file declares it",
                )
            )

    return DriftReport(
        issues=tuple(issues),
        warnings=tuple(warnings),
        sources_checked=sources_checked,
        declared_rules=declared_total,
        implemented_rules=implemented_total,
    )


def check(
    contracts_dir: Path | None = None,
    *,
    source_tables: Mapping[str, str] | None = None,
    reference_columns: Mapping[str, tuple[tuple[str, str], ...]] | None = None,
    partition_columns: Mapping[str, str] | None = None,
) -> bool:
    """Convenience boolean gate (True when no drift is detected)."""
    return run_drift_report(
        contracts_dir,
        source_tables=source_tables,
        reference_columns=reference_columns,
        partition_columns=partition_columns,
    ).passed


__all__ = [
    "BRONZE_ONLY_SOURCES",
    "DRIFT_KINDS",
    "DeclaredRule",
    "DriftIssue",
    "DriftReport",
    "check",
    "classifiable_declared",
    "run_drift_report",
]
