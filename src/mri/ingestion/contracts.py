"""Loading and validating versioned source contracts (YAML).

A contract describes the expected shape of one source file: its format, its
columns (name/type/required/nullable) and its primary key. Phase 04 enforces
column presence and basic types; richer quality rules are declared here for the
future Great Expectations gate but are not executed in this phase.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from mri.ingestion.config import find_repo_root
from mri.ingestion.exceptions import ContractError, ContractNotFoundError

COLUMN_TYPES: frozenset[str] = frozenset({"string", "integer", "number", "boolean", "date"})
SUPPORTED_FORMATS: frozenset[str] = frozenset({"csv", "json"})
SUPPORTED_CONTRACT_MAJOR = 1


@dataclass(frozen=True)
class Column:
    """One declared column in a contract."""

    name: str
    type: str
    required: bool
    nullable: bool
    description: str = ""


@dataclass(frozen=True)
class Contract:
    """A parsed and self-validated source contract."""

    id: str
    version: str
    owner: str
    source: str
    description: str
    format_types: tuple[str, ...]
    encoding: str
    delimiter: str
    primary_key: tuple[str, ...]
    columns: tuple[Column, ...]
    path: Path
    raw: Mapping[str, Any]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    def column_map(self) -> dict[str, Column]:
        return {column.name: column for column in self.columns}

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.required)

    @property
    def version_tuple(self) -> tuple[int, int, int]:
        return parse_version(self.version)


def parse_version(value: str) -> tuple[int, int, int]:
    """Parse a semantic version string like ``1.0.0``."""
    parts = value.strip().split(".")
    if len(parts) != 3 or not all(part.isdigit() for part in parts):
        raise ContractError(f"invalid semantic version {value!r}; expected MAJOR.MINOR.PATCH")
    return int(parts[0]), int(parts[1]), int(parts[2])


def default_contracts_dir() -> Path:
    return find_repo_root() / "data_contracts"


def _as_mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractError(f"{where} must be a mapping")
    return value


def _parse_columns(raw_columns: Any, where: str) -> tuple[Column, ...]:
    if not isinstance(raw_columns, Sequence) or isinstance(raw_columns, (str, bytes)):
        raise ContractError(f"{where}.columns must be a list")
    if not raw_columns:
        raise ContractError(f"{where}.columns must not be empty")
    columns: list[Column] = []
    seen: set[str] = set()
    for index, item in enumerate(raw_columns):
        entry = _as_mapping(item, f"{where}.columns[{index}]")
        name = str(entry.get("name", "")).strip()
        if not name:
            raise ContractError(f"{where}.columns[{index}] is missing a name")
        if name in seen:
            raise ContractError(f"{where}.columns has duplicate column {name!r}")
        seen.add(name)
        ctype = str(entry.get("type", "")).strip()
        if ctype not in COLUMN_TYPES:
            raise ContractError(f"{where}.columns[{index}] ({name}) has unsupported type {ctype!r}")
        columns.append(
            Column(
                name=name,
                type=ctype,
                required=bool(entry.get("required", True)),
                nullable=bool(entry.get("nullable", False)),
                description=str(entry.get("description", "")),
            )
        )
    return tuple(columns)


def _parse_contract(data: Any, path: Path) -> Contract:
    root = _as_mapping(data, str(path))
    where = path.name
    identifier = str(root.get("id", "")).strip()
    if not identifier:
        raise ContractError(f"{where} is missing an id")
    version = str(root.get("version", "")).strip()
    parse_version(version)
    if parse_version(version)[0] != SUPPORTED_CONTRACT_MAJOR:
        raise ContractError(
            f"{where} declares unsupported major version {version!r}; "
            f"this tool understands major {SUPPORTED_CONTRACT_MAJOR}"
        )

    fmt = _as_mapping(root.get("format", {}), f"{where}.format")
    raw_types = fmt.get("types", ["csv"])
    if not isinstance(raw_types, Sequence) or isinstance(raw_types, (str, bytes)):
        raise ContractError(f"{where}.format.types must be a list")
    format_types = tuple(str(item).strip() for item in raw_types)
    unknown = [item for item in format_types if item not in SUPPORTED_FORMATS]
    if unknown:
        raise ContractError(f"{where}.format.types contains unsupported formats {unknown}")

    keys = _as_mapping(root.get("keys", {}), f"{where}.keys")
    raw_pk = keys.get("primary", [])
    if not isinstance(raw_pk, Sequence) or isinstance(raw_pk, (str, bytes)) or not raw_pk:
        raise ContractError(f"{where}.keys.primary must be a non-empty list")
    primary_key = tuple(str(item).strip() for item in raw_pk)

    columns = _parse_columns(root.get("columns"), where)
    known = {column.name for column in columns}
    missing_pk = [col for col in primary_key if col not in known]
    if missing_pk:
        raise ContractError(f"{where}.keys.primary references unknown columns {missing_pk}")

    return Contract(
        id=identifier,
        version=version,
        owner=str(root.get("owner", "unknown")),
        source=str(root.get("source", "")),
        description=str(root.get("description", "")),
        format_types=format_types,
        encoding=str(fmt.get("encoding", "utf-8")),
        delimiter=str(fmt.get("delimiter", ",")),
        primary_key=primary_key,
        columns=columns,
        path=path,
        raw=root,
    )


def load_contract(ref: str | Path, contracts_dir: Path | None = None) -> Contract:
    """Load a contract by source id (``sales``) or by explicit file path."""
    candidate = Path(ref)
    if candidate.suffix in {".yml", ".yaml"} or candidate.is_file():
        path = candidate
        if not path.is_file():
            raise ContractNotFoundError(f"contract file not found: {path}")
    else:
        base = contracts_dir or default_contracts_dir()
        path = base / f"{ref}.yml"
        if not path.is_file():
            raise ContractNotFoundError(f"no contract for source {ref!r} (looked for {path})")
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as exc:  # pragma: no cover - defensive
        raise ContractError(f"invalid YAML in {path}: {exc}") from exc
    return _parse_contract(data, path)


def load_all_contracts(contracts_dir: Path | None = None) -> dict[str, Contract]:
    """Load every ``*.yml`` contract in a directory, keyed by contract id."""
    base = contracts_dir or default_contracts_dir()
    if not base.is_dir():
        raise ContractNotFoundError(f"contracts directory not found: {base}")
    contracts: dict[str, Contract] = {}
    for path in sorted(base.glob("*.yml")):
        contract = load_contract(path, base)
        if contract.id in contracts:
            raise ContractError(f"duplicate contract id {contract.id!r}")
        contracts[contract.id] = contract
    return contracts
