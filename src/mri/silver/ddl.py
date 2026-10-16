"""Table specifications and DDL for the ``raw_staging``, ``silver`` and ``audit`` schemas.

Silver columns are derived from the source contracts and are never invented:
for every source the business columns are exactly the contract columns, plus
the Bronze provenance columns and the Silver ``batch_id``.  ``raw_staging``
tables are constraint-free heaps for fast bulk ``COPY``; ``silver`` tables carry
the contract primary keys, foreign keys and structural checks; the ``audit``
schema holds the load registry and the per-batch load log.

The ``weather`` table merges the ``open_meteo_history`` and
``open_meteo_forecast`` sources: both date columns are nullable and uniqueness
of each natural key is enforced through one partial unique index per source,
so the table has no single-column-set primary key.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from mri.ingestion.contracts import Contract
from mri.ingestion.exceptions import SilverLoadError

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

PG_TYPES: dict[str, str] = {
    "string": "TEXT",
    "integer": "BIGINT",
    "number": "DOUBLE PRECISION",
    "boolean": "BOOLEAN",
    "date": "DATE",
}

SOURCE_TABLES: dict[str, str] = {
    "calendar": "calendar",
    "channels": "channels",
    "customers": "customers",
    "frankfurter_fx": "fx_rates",
    "inventory": "inventory",
    "open_meteo_forecast": "weather",
    "open_meteo_history": "weather",
    "openfoodfacts_products": "external_products",
    "products": "products",
    "promotions": "promotions",
    "purchases": "purchases",
    "regions": "regions",
    "sales": "sales",
    "stores": "stores",
}

TABLE_LOAD_ORDER: tuple[str, ...] = (
    "regions",
    "channels",
    "calendar",
    "stores",
    "products",
    "customers",
    "promotions",
    "sales",
    "purchases",
    "inventory",
    "weather",
    "fx_rates",
    "external_products",
)


@dataclass(frozen=True)
class ColumnSpec:
    """One physical column of a staging or silver table."""

    name: str
    pg_type: str
    nullable: bool


@dataclass(frozen=True)
class ForeignKey:
    """A foreign key from one silver table to another."""

    column: str
    ref_table: str
    ref_column: str


@dataclass(frozen=True)
class Check:
    """A named structural check constraint (business rules stay in GX)."""

    name: str
    expression: str


@dataclass(frozen=True)
class TableSpec:
    """Everything the loader needs to know about one target table."""

    name: str
    sources: tuple[str, ...]
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...]
    checks: tuple[Check, ...]
    conflict_keys: Mapping[str, tuple[str, ...]]

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)

    @property
    def column_types(self) -> dict[str, str]:
        return {column.name: column.pg_type for column in self.columns}

    def column(self, name: str) -> ColumnSpec:
        for column in self.columns:
            if column.name == name:
                return column
        raise SilverLoadError(f"table {self.name!r} has no column {name!r}")


FOREIGN_KEYS: dict[str, tuple[ForeignKey, ...]] = {
    "stores": (ForeignKey("region_id", "regions", "region_id"),),
    "customers": (
        ForeignKey("region_id", "regions", "region_id"),
        ForeignKey("preferred_channel_id", "channels", "channel_id"),
    ),
    "promotions": (
        ForeignKey("product_id", "products", "product_id"),
        ForeignKey("region_id", "regions", "region_id"),
        ForeignKey("channel_id", "channels", "channel_id"),
    ),
    "sales": (
        ForeignKey("store_id", "stores", "store_id"),
        ForeignKey("channel_id", "channels", "channel_id"),
        ForeignKey("customer_id", "customers", "customer_id"),
        ForeignKey("product_id", "products", "product_id"),
        ForeignKey("promotion_id", "promotions", "promotion_id"),
    ),
    "purchases": (
        ForeignKey("store_id", "stores", "store_id"),
        ForeignKey("product_id", "products", "product_id"),
    ),
    "inventory": (
        ForeignKey("store_id", "stores", "store_id"),
        ForeignKey("product_id", "products", "product_id"),
    ),
    "weather": (ForeignKey("region_id", "regions", "region_id"),),
    "external_products": (ForeignKey("internal_product_id", "products", "product_id"),),
}

TABLE_CHECKS: dict[str, tuple[Check, ...]] = {
    "weather": (
        Check(
            "ck_weather_dates",
            '"observation_date" IS NOT NULL OR "forecast_date" IS NOT NULL',
        ),
    ),
}

PROVENANCE_COLUMNS: tuple[ColumnSpec, ...] = (
    ColumnSpec("source", "TEXT", False),
    ColumnSpec("source_file", "TEXT", False),
    ColumnSpec("source_hash", "TEXT", False),
    ColumnSpec("batch_id", "TEXT", False),
    ColumnSpec("ingestion_run_id", "TEXT", False),
    ColumnSpec("ingestion_timestamp", "TIMESTAMPTZ", False),
)

AUDIT_TABLE = "load_audit"

AUDIT_STATUSES: tuple[str, ...] = ("success", "failed", "skipped")


def ident(name: str) -> str:
    """Validate and double-quote one SQL identifier."""
    if not IDENTIFIER_RE.match(name or ""):
        raise SilverLoadError(f"unsafe SQL identifier {name!r}")
    return f'"{name}"'


def qualified(schema: str, table: str) -> str:
    return f"{ident(schema)}.{ident(table)}"


def build_table_specs(contracts: Mapping[str, Contract]) -> dict[str, TableSpec]:
    """Build one :class:`TableSpec` per target table from the source contracts."""
    missing = sorted(set(SOURCE_TABLES) - set(contracts))
    if missing:
        raise SilverLoadError(f"contracts missing for sources {missing}")
    unknown = sorted(set(contracts) - set(SOURCE_TABLES))
    if unknown:
        raise SilverLoadError(f"no silver table mapping for sources {unknown}")

    specs: dict[str, TableSpec] = {}
    for table in TABLE_LOAD_ORDER:
        sources = tuple(
            sorted(source for source, target in SOURCE_TABLES.items() if target == table)
        )
        merged = _merge_columns(table, sources, contracts)
        primary_key = _merged_primary_key(sources, contracts)
        specs[table] = TableSpec(
            name=table,
            sources=sources,
            columns=merged,
            primary_key=primary_key,
            foreign_keys=FOREIGN_KEYS.get(table, ()),
            checks=TABLE_CHECKS.get(table, ()),
            conflict_keys={source: contracts[source].primary_key for source in sources},
        )
    _validate_specs(specs)
    return specs


def _merge_columns(
    table: str,
    sources: tuple[str, ...],
    contracts: Mapping[str, Contract],
) -> tuple[ColumnSpec, ...]:
    merged: dict[str, ColumnSpec] = {}
    holders: dict[str, set[str]] = {}
    for source in sources:
        for column in contracts[source].columns:
            pg_type = PG_TYPES.get(column.type)
            if pg_type is None:
                raise SilverLoadError(
                    f"source {source!r} column {column.name!r} has unsupported type {column.type!r}"
                )
            existing = merged.get(column.name)
            if existing is None:
                merged[column.name] = ColumnSpec(column.name, pg_type, column.nullable)
            else:
                if existing.pg_type != pg_type:
                    raise SilverLoadError(
                        f"table {table!r}: column {column.name!r} has conflicting types "
                        f"{existing.pg_type} and {pg_type}"
                    )
                if column.nullable and not existing.nullable:
                    merged[column.name] = ColumnSpec(column.name, pg_type, True)
            holders.setdefault(column.name, set()).add(source)
    for name, seen in holders.items():
        if len(seen) < len(sources) and not merged[name].nullable:
            partial = merged[name]
            merged[name] = ColumnSpec(partial.name, partial.pg_type, True)
    for provenance in PROVENANCE_COLUMNS:
        if provenance.name in merged:
            raise SilverLoadError(
                f"table {table!r}: contract column {provenance.name!r} clashes with provenance"
            )
        merged[provenance.name] = provenance
    return tuple(merged.values())


def _merged_primary_key(
    sources: tuple[str, ...],
    contracts: Mapping[str, Contract],
) -> tuple[str, ...]:
    keys = [contracts[source].primary_key for source in sources]
    first = keys[0]
    if all(candidate == first for candidate in keys):
        return first
    return ()


def _validate_specs(specs: Mapping[str, TableSpec]) -> None:
    for table, spec in specs.items():
        known = set(spec.column_names)
        for fk in spec.foreign_keys:
            if fk.column not in known:
                raise SilverLoadError(f"table {table!r}: foreign key column {fk.column!r} missing")
            ref = specs.get(fk.ref_table)
            if ref is None:
                raise SilverLoadError(
                    f"table {table!r}: foreign key references unknown table {fk.ref_table!r}"
                )
            if fk.ref_column not in ref.column_names:
                raise SilverLoadError(
                    f"table {table!r}: foreign key references missing column "
                    f"{fk.ref_table!r}.{fk.ref_column!r}"
                )
        if not spec.primary_key and not spec.conflict_keys:
            raise SilverLoadError(f"table {table!r} has neither a primary key nor conflict keys")
        for source, keys in spec.conflict_keys.items():
            unknown_keys = [key for key in keys if key not in known]
            if unknown_keys:
                raise SilverLoadError(
                    f"table {table!r}: conflict keys for source {source!r} missing {unknown_keys}"
                )


def conflict_predicate(spec: TableSpec, keys: tuple[str, ...]) -> str:
    """``IS NOT NULL`` predicate shared by the partial unique index and ON CONFLICT."""
    if spec.primary_key:
        return ""
    nullable_keys = [key for key in keys if spec.column(key).nullable]
    return " AND ".join(f"{ident(key)} IS NOT NULL" for key in nullable_keys)


def conflict_clause(spec: TableSpec, source: str) -> str:
    """Render the ``ON CONFLICT ... DO UPDATE`` clause for one source."""
    keys = spec.conflict_keys.get(source)
    if keys is None:
        raise SilverLoadError(f"table {spec.name!r} has no conflict keys for source {source!r}")
    columns = ", ".join(ident(key) for key in keys)
    predicate = conflict_predicate(spec, keys)
    where = f" WHERE {predicate}" if predicate else ""
    updatable = [name for name in spec.column_names if name not in keys]
    assignments = ", ".join(f"{ident(name)} = EXCLUDED.{ident(name)}" for name in updatable)
    return f"ON CONFLICT ({columns}){where} DO UPDATE SET {assignments}"


def _ordered(specs: Mapping[str, TableSpec]) -> list[TableSpec]:
    missing = sorted(set(TABLE_LOAD_ORDER) - set(specs))
    extra = sorted(set(specs) - set(TABLE_LOAD_ORDER))
    if missing or extra:
        raise SilverLoadError(f"table specs mismatch (missing={missing}, extra={extra})")
    return [specs[table] for table in TABLE_LOAD_ORDER]


def render_staging_ddl(specs: Mapping[str, TableSpec], staging_schema: str) -> list[str]:
    """Constraint-free staging tables: same columns as silver, no keys at all."""
    statements = [f"CREATE SCHEMA IF NOT EXISTS {ident(staging_schema)}"]
    for spec in _ordered(specs):
        body = ", ".join(f"{ident(column.name)} {column.pg_type}" for column in spec.columns)
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {qualified(staging_schema, spec.name)} ({body})"
        )
    return statements


def render_silver_ddl(specs: Mapping[str, TableSpec], silver_schema: str) -> list[str]:
    """Silver tables with primary keys, structural checks, foreign keys and indexes."""
    statements = [f"CREATE SCHEMA IF NOT EXISTS {ident(silver_schema)}"]
    for spec in _ordered(specs):
        parts: list[str] = []
        for column in spec.columns:
            piece = f"{ident(column.name)} {column.pg_type}"
            if not column.nullable:
                piece += " NOT NULL"
            parts.append(piece)
        if spec.primary_key:
            parts.append(f"PRIMARY KEY ({', '.join(ident(key) for key in spec.primary_key)})")
        for check in spec.checks:
            parts.append(f"CONSTRAINT {ident(check.name)} CHECK ({check.expression})")
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {qualified(silver_schema, spec.name)} ({', '.join(parts)})"
        )
    for spec in _ordered(specs):
        statements.extend(_foreign_key_statements(spec, silver_schema))
        statements.extend(_unique_key_indexes(spec, silver_schema))
    return statements


def _foreign_key_statements(spec: TableSpec, silver_schema: str) -> list[str]:
    statements: list[str] = []
    for fk in spec.foreign_keys:
        constraint = f"fk_{spec.name}_{fk.column}"
        table = qualified(silver_schema, spec.name)
        ref = qualified(silver_schema, fk.ref_table)
        statements.append(
            "DO $$ BEGIN "
            f"IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{constraint}' "
            f"AND conrelid = '{table}'::regclass AND contype = 'f') THEN "
            f"ALTER TABLE {table} ADD CONSTRAINT {ident(constraint)} "
            f"FOREIGN KEY ({ident(fk.column)}) REFERENCES {ref} ({ident(fk.ref_column)}); "
            "END IF; END $$;"
        )
    return statements


def _unique_key_indexes(spec: TableSpec, silver_schema: str) -> list[str]:
    if spec.primary_key:
        return []
    statements: list[str] = []
    seen: set[tuple[str, ...]] = set()
    for source in spec.sources:
        keys = spec.conflict_keys[source]
        if keys in seen:
            continue
        seen.add(keys)
        columns = ", ".join(ident(key) for key in keys)
        predicate = conflict_predicate(spec, keys)
        where = f" WHERE {predicate}" if predicate else ""
        name = ident(f"ux_{spec.name}_{source}")
        statements.append(
            f"CREATE UNIQUE INDEX IF NOT EXISTS {name} "
            f"ON {qualified(silver_schema, spec.name)} ({columns}){where}"
        )
    return statements


def render_audit_ddl(audit_schema: str) -> list[str]:
    """The audit schema: per-batch load log doubling as the load registry."""
    table = qualified(audit_schema, AUDIT_TABLE)
    statuses = ", ".join(f"'{status}'" for status in AUDIT_STATUSES)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {ident(audit_schema)}",
        (
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "load_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
            "run_id TEXT NOT NULL, "
            "batch_id TEXT NOT NULL, "
            "source TEXT NOT NULL, "
            "target_table TEXT NOT NULL, "
            "started_at TIMESTAMPTZ NOT NULL, "
            "finished_at TIMESTAMPTZ, "
            f"status TEXT NOT NULL CONSTRAINT ck_{AUDIT_TABLE}_status "
            f"CHECK (status IN ({statuses})), "
            "rows_read BIGINT NOT NULL DEFAULT 0, "
            "rows_inserted BIGINT NOT NULL DEFAULT 0, "
            "rows_rejected BIGINT NOT NULL DEFAULT 0, "
            "error_message TEXT)"
        ),
        (
            f"CREATE UNIQUE INDEX IF NOT EXISTS ux_{AUDIT_TABLE}_success ON {table} "
            "(batch_id, target_table) WHERE status = 'success'"
        ),
        f"CREATE INDEX IF NOT EXISTS ix_{AUDIT_TABLE}_run ON {table} (run_id)",
        f"CREATE INDEX IF NOT EXISTS ix_{AUDIT_TABLE}_target ON {table} (target_table)",
    ]


def render_schema_ddl(
    specs: Mapping[str, TableSpec],
    *,
    staging_schema: str,
    silver_schema: str,
    audit_schema: str,
) -> list[str]:
    """Every statement required to bring all three schemas to their declared state."""
    return [
        *render_staging_ddl(specs, staging_schema),
        *render_silver_ddl(specs, silver_schema),
        *render_audit_ddl(audit_schema),
    ]


__all__ = [
    "AUDIT_STATUSES",
    "AUDIT_TABLE",
    "FOREIGN_KEYS",
    "PG_TYPES",
    "PROVENANCE_COLUMNS",
    "SOURCE_TABLES",
    "TABLE_CHECKS",
    "TABLE_LOAD_ORDER",
    "Check",
    "ColumnSpec",
    "ForeignKey",
    "TableSpec",
    "build_table_specs",
    "conflict_clause",
    "conflict_predicate",
    "ident",
    "qualified",
    "render_audit_ddl",
    "render_schema_ddl",
    "render_silver_ddl",
    "render_staging_ddl",
]
