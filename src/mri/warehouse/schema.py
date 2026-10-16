"""Star-schema specifications, DDL and load statements for the warehouse.

The ``warehouse`` schema is a Kimball-style star schema built from the Silver
layer: seven dimensions (``dim_date``, ``dim_client``, ``dim_produit``,
``dim_magasin``, ``dim_region``, ``dim_canal``, ``dim_promotion``) and three
facts (``fact_ventes``, ``fact_achats``, ``fact_stock``), each with a
documented grain, surrogate keys and conformed key columns.

Column policy — no measure is ever fabricated:

* business/attribute columns are verbatim Silver source columns;
* ``*_sk`` columns are surrogate keys (``GENERATED ALWAYS AS IDENTITY``) and
  resolved surrogate foreign keys;
* the only derived column is ``dim_date.day`` (``EXTRACT(DAY FROM date)``),
  declared in :data:`DERIVED_COLUMNS`;
* measures are exactly the numeric source columns of the fact streams, listed
  per table in :data:`RECONCILIATION_MEASURES`.

Rebuild semantics: every build truncates all warehouse tables
(``RESTART IDENTITY``) and re-inserts rows ordered by business key, so an
identical Silver snapshot always yields identical surrogate keys.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from mri.ingestion.exceptions import WarehouseError

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

DIMENSION_ORDER: tuple[str, ...] = (
    "dim_region",
    "dim_canal",
    "dim_date",
    "dim_magasin",
    "dim_client",
    "dim_produit",
    "dim_promotion",
)

FACT_ORDER: tuple[str, ...] = (
    "fact_ventes",
    "fact_achats",
    "fact_stock",
)


@dataclass(frozen=True)
class Column:
    """One physical warehouse column with an optional inline check."""

    name: str
    pg_type: str
    nullable: bool = False
    check: str | None = None
    identity: bool = False


@dataclass(frozen=True)
class ForeignKey:
    """A foreign key from one warehouse table to another (always ``*_sk``)."""

    column: str
    ref_table: str
    ref_column: str
    create_index: bool = True


@dataclass(frozen=True)
class WarehouseTable:
    """Everything the builder and the docs need for one star-schema table."""

    name: str
    kind: str
    source: str
    grain: str
    surrogate_key: str | None
    business_key: tuple[str, ...]
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKey, ...] = ()
    unique_keys: tuple[tuple[str, ...], ...] = ()
    measures: tuple[str, ...] = ()
    degenerate: tuple[str, ...] = ()

    @property
    def column_names(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns)


DERIVED_COLUMNS: dict[str, dict[str, str]] = {
    "dim_date": {
        "day": "EXTRACT(DAY FROM date): day of month, deterministic from the date",
    },
}

TABLES: tuple[WarehouseTable, ...] = (
    WarehouseTable(
        name="dim_region",
        kind="dimension",
        source="regions",
        grain="one row per Moroccan region (business key region_id)",
        surrogate_key="region_sk",
        business_key=("region_id",),
        columns=(
            Column("region_sk", "BIGINT", identity=True),
            Column("region_id", "TEXT"),
            Column("region_name", "TEXT"),
            Column(
                "demand_weight",
                "DOUBLE PRECISION",
                check='"demand_weight" >= 0 AND "demand_weight" <= 1',
            ),
        ),
        primary_key=("region_sk",),
        unique_keys=(("region_id",),),
    ),
    WarehouseTable(
        name="dim_canal",
        kind="dimension",
        source="channels",
        grain="one row per sales channel (business key channel_id)",
        surrogate_key="canal_sk",
        business_key=("channel_id",),
        columns=(
            Column("canal_sk", "BIGINT", identity=True),
            Column("channel_id", "TEXT"),
            Column("channel_name", "TEXT"),
            Column(
                "channel_type",
                "TEXT",
                check="\"channel_type\" IN ('physical', 'digital')",
            ),
            Column(
                "demand_weight",
                "DOUBLE PRECISION",
                check='"demand_weight" >= 0 AND "demand_weight" <= 1',
            ),
        ),
        primary_key=("canal_sk",),
        unique_keys=(("channel_id",),),
    ),
    WarehouseTable(
        name="dim_date",
        kind="dimension",
        source="calendar",
        grain="one row per calendar date (business key date), 2021-01-01 to 2024-12-31",
        surrogate_key="date_sk",
        business_key=("date",),
        columns=(
            Column("date_sk", "BIGINT", identity=True),
            Column("date", "DATE"),
            Column("day", "BIGINT", check='"day" >= 1 AND "day" <= 31'),
            Column("year", "BIGINT"),
            Column("month", "BIGINT", check='"month" >= 1 AND "month" <= 12'),
            Column("quarter", "BIGINT", check='"quarter" >= 1 AND "quarter" <= 4'),
            Column("iso_week", "BIGINT", check='"iso_week" >= 1 AND "iso_week" <= 53'),
            Column("weekday", "BIGINT", check='"weekday" >= 0 AND "weekday" <= 6'),
            Column("weekday_name", "TEXT"),
            Column("is_weekend", "BOOLEAN", check='"is_weekend" = ("weekday" >= 5)'),
            Column(
                "holiday_name",
                "TEXT",
                nullable=True,
                check='("is_holiday" AND "holiday_name" IS NOT NULL) '
                'OR (NOT "is_holiday" AND "holiday_name" IS NULL)',
            ),
            Column("is_holiday", "BOOLEAN"),
            Column("is_ramadan", "BOOLEAN"),
            Column("is_eid_al_fitr", "BOOLEAN"),
            Column("is_eid_al_adha", "BOOLEAN"),
            Column(
                "season",
                "TEXT",
                check="\"season\" IN ('winter', 'spring', 'summer', 'autumn')",
            ),
        ),
        primary_key=("date_sk",),
        unique_keys=(("date",),),
    ),
    WarehouseTable(
        name="dim_magasin",
        kind="dimension",
        source="stores",
        grain="one row per retail store (business key store_id)",
        surrogate_key="magasin_sk",
        business_key=("store_id",),
        columns=(
            Column("magasin_sk", "BIGINT", identity=True),
            Column("store_id", "TEXT"),
            Column("store_name", "TEXT"),
            Column("city", "TEXT"),
            Column("region_id", "TEXT"),
            Column("region_sk", "BIGINT"),
            Column(
                "store_format",
                "TEXT",
                check="\"store_format\" IN ('hypermarket', 'supermarket', "
                "'convenience', 'cash_and_carry')",
            ),
            Column("size_sqm", "BIGINT", check='"size_sqm" > 0'),
            Column("open_date", "DATE"),
            Column("is_active", "BOOLEAN"),
        ),
        primary_key=("magasin_sk",),
        foreign_keys=(ForeignKey("region_sk", "dim_region", "region_sk"),),
        unique_keys=(("store_id",),),
    ),
    WarehouseTable(
        name="dim_client",
        kind="dimension",
        source="customers",
        grain="one row per customer (business key customer_id)",
        surrogate_key="client_sk",
        business_key=("customer_id",),
        columns=(
            Column("client_sk", "BIGINT", identity=True),
            Column("customer_id", "TEXT"),
            Column("signup_date", "DATE"),
            Column("region_id", "TEXT"),
            Column("region_sk", "BIGINT"),
            Column("birth_year", "BIGINT", check='"birth_year" >= 1900 AND "birth_year" <= 2024'),
            Column("gender", "TEXT", check="\"gender\" IN ('F', 'M', 'U')"),
            Column(
                "segment",
                "TEXT",
                check="\"segment\" IN ('jeune', 'famille', 'senior', 'actif')",
            ),
            Column(
                "loyalty_tier",
                "TEXT",
                check="\"loyalty_tier\" IN ('bronze', 'silver', 'gold', 'platinum')",
            ),
            Column("preferred_channel_id", "TEXT"),
            Column("canal_sk", "BIGINT"),
            Column("is_active", "BOOLEAN"),
        ),
        primary_key=("client_sk",),
        foreign_keys=(
            ForeignKey("region_sk", "dim_region", "region_sk"),
            ForeignKey("canal_sk", "dim_canal", "canal_sk"),
        ),
        unique_keys=(("customer_id",),),
    ),
    WarehouseTable(
        name="dim_produit",
        kind="dimension",
        source="products",
        grain="one row per product (business key product_id)",
        surrogate_key="produit_sk",
        business_key=("product_id",),
        columns=(
            Column("produit_sk", "BIGINT", identity=True),
            Column("product_id", "TEXT"),
            Column("product_name", "TEXT"),
            Column("category", "TEXT"),
            Column("subcategory", "TEXT"),
            Column("brand", "TEXT"),
            Column("unit_cost_mad", "DOUBLE PRECISION", check='"unit_cost_mad" > 0'),
            Column(
                "unit_price_mad",
                "DOUBLE PRECISION",
                check='"unit_price_mad" >= "unit_cost_mad"',
            ),
            Column(
                "margin_pct",
                "DOUBLE PRECISION",
                check='"margin_pct" >= 0 AND "margin_pct" < 1',
            ),
            Column("launch_date", "DATE"),
            Column("is_active", "BOOLEAN"),
        ),
        primary_key=("produit_sk",),
        unique_keys=(("product_id",),),
    ),
    WarehouseTable(
        name="dim_promotion",
        kind="dimension",
        source="promotions",
        grain="one row per promotion (business key promotion_id)",
        surrogate_key="promotion_sk",
        business_key=("promotion_id",),
        columns=(
            Column("promotion_sk", "BIGINT", identity=True),
            Column("promotion_id", "TEXT"),
            Column("product_id", "TEXT"),
            Column("produit_sk", "BIGINT"),
            Column("region_id", "TEXT", nullable=True),
            Column("region_sk", "BIGINT", nullable=True),
            Column("channel_id", "TEXT", nullable=True),
            Column("canal_sk", "BIGINT", nullable=True),
            Column(
                "promotion_type",
                "TEXT",
                check="\"promotion_type\" IN ('remise', 'prix_promo', 'bogo', "
                "'fidelite', 'bundle')",
            ),
            Column(
                "discount_pct",
                "DOUBLE PRECISION",
                check='"discount_pct" >= 0 AND "discount_pct" <= 1',
            ),
            Column("start_date", "DATE"),
            Column("end_date", "DATE", check='"end_date" >= "start_date"'),
        ),
        primary_key=("promotion_sk",),
        foreign_keys=(
            ForeignKey("produit_sk", "dim_produit", "produit_sk"),
            ForeignKey("region_sk", "dim_region", "region_sk"),
            ForeignKey("canal_sk", "dim_canal", "canal_sk"),
        ),
        unique_keys=(("promotion_id",),),
    ),
    WarehouseTable(
        name="fact_ventes",
        kind="fact",
        source="sales",
        grain="one row per sale line item: one product line inside one shopping "
        "transaction (business key sale_id; transaction_id groups lines)",
        surrogate_key=None,
        business_key=("sale_id",),
        columns=(
            Column("date_sk", "BIGINT"),
            Column("magasin_sk", "BIGINT"),
            Column("canal_sk", "BIGINT"),
            Column("client_sk", "BIGINT", nullable=True),
            Column("produit_sk", "BIGINT"),
            Column("promotion_sk", "BIGINT", nullable=True),
            Column("sale_id", "TEXT"),
            Column("transaction_id", "TEXT"),
            Column("hour_of_day", "BIGINT", check='"hour_of_day" >= 8 AND "hour_of_day" <= 23'),
            Column("quantity", "BIGINT", check='"quantity" >= 1'),
            Column("unit_price_mad", "DOUBLE PRECISION", check='"unit_price_mad" > 0'),
            Column(
                "discount_pct",
                "DOUBLE PRECISION",
                check='"discount_pct" >= 0 AND "discount_pct" <= 1',
            ),
            Column("net_amount_mad", "DOUBLE PRECISION", check='"net_amount_mad" >= 0'),
            Column("cost_amount_mad", "DOUBLE PRECISION", check='"cost_amount_mad" >= 0'),
            Column("margin_mad", "DOUBLE PRECISION", check='"margin_mad" >= 0'),
        ),
        primary_key=("sale_id",),
        foreign_keys=(
            ForeignKey("date_sk", "dim_date", "date_sk"),
            ForeignKey("magasin_sk", "dim_magasin", "magasin_sk"),
            ForeignKey("canal_sk", "dim_canal", "canal_sk"),
            ForeignKey("client_sk", "dim_client", "client_sk"),
            ForeignKey("produit_sk", "dim_produit", "produit_sk"),
            ForeignKey("promotion_sk", "dim_promotion", "promotion_sk"),
        ),
        measures=(
            "quantity",
            "unit_price_mad",
            "discount_pct",
            "net_amount_mad",
            "cost_amount_mad",
            "margin_mad",
        ),
        degenerate=("transaction_id", "hour_of_day"),
    ),
    WarehouseTable(
        name="fact_achats",
        kind="fact",
        source="purchases",
        grain="one row per purchase receipt line: one product received by one "
        "store on one date (business key purchase_id)",
        surrogate_key=None,
        business_key=("purchase_id",),
        columns=(
            Column("date_sk", "BIGINT"),
            Column("magasin_sk", "BIGINT"),
            Column("produit_sk", "BIGINT"),
            Column("purchase_id", "TEXT"),
            Column("quantity", "BIGINT", check='"quantity" >= 1'),
            Column("unit_cost_mad", "DOUBLE PRECISION", check='"unit_cost_mad" > 0'),
            Column("total_cost_mad", "DOUBLE PRECISION", check='"total_cost_mad" >= 0'),
        ),
        primary_key=("purchase_id",),
        foreign_keys=(
            ForeignKey("date_sk", "dim_date", "date_sk"),
            ForeignKey("magasin_sk", "dim_magasin", "magasin_sk"),
            ForeignKey("produit_sk", "dim_produit", "produit_sk"),
        ),
        measures=("quantity", "unit_cost_mad", "total_cost_mad"),
        degenerate=(),
    ),
    WarehouseTable(
        name="fact_stock",
        kind="fact",
        source="inventory",
        grain="one row per stock snapshot: one store's stock level for one "
        "product on one snapshot date (grain = date_sk x magasin_sk x produit_sk)",
        surrogate_key=None,
        business_key=("date_sk", "magasin_sk", "produit_sk"),
        columns=(
            Column("date_sk", "BIGINT"),
            Column("magasin_sk", "BIGINT"),
            Column("produit_sk", "BIGINT"),
            Column("quantity_on_hand", "BIGINT", check='"quantity_on_hand" >= 0'),
            Column("stock_value_mad", "DOUBLE PRECISION", check='"stock_value_mad" >= 0'),
            Column(
                "stock_status",
                "TEXT",
                check="\"stock_status\" IN ('in_stock', 'low', 'out_of_stock')",
            ),
        ),
        primary_key=("date_sk", "magasin_sk", "produit_sk"),
        foreign_keys=(
            ForeignKey("date_sk", "dim_date", "date_sk", create_index=False),
            ForeignKey("magasin_sk", "dim_magasin", "magasin_sk"),
            ForeignKey("produit_sk", "dim_produit", "produit_sk"),
        ),
        measures=("quantity_on_hand", "stock_value_mad"),
        degenerate=("stock_status",),
    ),
)

DIMENSIONS: tuple[WarehouseTable, ...] = tuple(
    table for table in TABLES if table.kind == "dimension"
)
FACTS: tuple[WarehouseTable, ...] = tuple(table for table in TABLES if table.kind == "fact")
TABLE_BY_NAME: dict[str, WarehouseTable] = {table.name: table for table in TABLES}


@dataclass(frozen=True)
class MeasureMetric:
    """One reconciled aggregate: sum of a Silver measure column vs warehouse."""

    name: str
    column: str
    money: bool = False


RECONCILIATION_MEASURES: dict[str, tuple[MeasureMetric, ...]] = {
    "fact_ventes": (
        MeasureMetric("quantity", "quantity"),
        MeasureMetric("net_amount_mad", "net_amount_mad", money=True),
        MeasureMetric("cost_amount_mad", "cost_amount_mad", money=True),
        MeasureMetric("margin_mad", "margin_mad", money=True),
    ),
    "fact_achats": (
        MeasureMetric("quantity", "quantity"),
        MeasureMetric("total_cost_mad", "total_cost_mad", money=True),
    ),
    "fact_stock": (
        MeasureMetric("quantity_on_hand", "quantity_on_hand"),
        MeasureMetric("stock_value_mad", "stock_value_mad", money=True),
    ),
}


def ident(name: str) -> str:
    """Validate and double-quote one SQL identifier."""
    if not IDENTIFIER_RE.match(name or ""):
        raise WarehouseError(f"unsafe SQL identifier {name!r}")
    return f'"{name}"'


def qualified(schema: str, table: str) -> str:
    return f"{ident(schema)}.{ident(table)}"


def _column_ddl(table: WarehouseTable, column: Column) -> str:
    piece = f"{ident(column.name)} {column.pg_type}"
    if column.identity:
        piece += " GENERATED ALWAYS AS IDENTITY"
    if not column.nullable:
        piece += " NOT NULL"
    if column.check is not None:
        piece += f" CONSTRAINT {ident(f'ck_{table.name}_{column.name}')} CHECK ({column.check})"
    return piece


def _table_ddl(table: WarehouseTable, warehouse_schema: str) -> str:
    parts = [_column_ddl(table, column) for column in table.columns]
    parts.append(f"PRIMARY KEY ({', '.join(ident(key) for key in table.primary_key)})")
    for keys in table.unique_keys:
        columns = ", ".join(ident(key) for key in keys)
        constraint = f"uq_{table.name}_{'_'.join(keys)}"
        parts.append(f"CONSTRAINT {ident(constraint)} UNIQUE ({columns})")
    body = ", ".join(parts)
    return f"CREATE TABLE IF NOT EXISTS {qualified(warehouse_schema, table.name)} ({body})"


def _foreign_key_statements(table: WarehouseTable, warehouse_schema: str) -> list[str]:
    statements: list[str] = []
    for fk in table.foreign_keys:
        constraint = f"fk_{table.name}_{fk.column}"
        target = qualified(warehouse_schema, table.name)
        ref = qualified(warehouse_schema, fk.ref_table)
        statements.append(
            "DO $$ BEGIN "
            f"IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = '{constraint}' "
            f"AND conrelid = '{target}'::regclass AND contype = 'f') THEN "
            f"ALTER TABLE {target} ADD CONSTRAINT {ident(constraint)} "
            f"FOREIGN KEY ({ident(fk.column)}) REFERENCES {ref} ({ident(fk.ref_column)}); "
            "END IF; END $$;"
        )
    return statements


def _index_statements(table: WarehouseTable, warehouse_schema: str) -> list[str]:
    statements: list[str] = []
    for fk in table.foreign_keys:
        if not fk.create_index:
            continue
        name = ident(f"ix_{table.name}_{fk.column}")
        statements.append(
            f"CREATE INDEX IF NOT EXISTS {name} "
            f"ON {qualified(warehouse_schema, table.name)} ({ident(fk.column)})"
        )
    return statements


def render_warehouse_ddl(*, warehouse_schema: str, silver_schema: str) -> list[str]:
    """Every statement that brings the warehouse schema to its declared state.

    ``silver_schema`` is validated but unused: DDL never touches Silver.
    """
    ident(silver_schema)
    statements = [f"CREATE SCHEMA IF NOT EXISTS {ident(warehouse_schema)}"]
    for table in TABLES:
        statements.append(_table_ddl(table, warehouse_schema))
    for table in TABLES:
        statements.extend(_foreign_key_statements(table, warehouse_schema))
    for table in FACTS:
        statements.extend(_index_statements(table, warehouse_schema))
    return statements


def render_truncate_statement(*, warehouse_schema: str) -> str:
    """One statement that empties every table and restarts identity sequences."""
    targets = ", ".join(qualified(warehouse_schema, table.name) for table in TABLES)
    return f"TRUNCATE {targets} RESTART IDENTITY CASCADE"


def render_load_statements(*, warehouse_schema: str, silver_schema: str) -> list[tuple[str, str]]:
    """Ordered ``(table, insert_sql)`` pairs: dimensions first, then facts.

    Every dimension is inserted ``ORDER BY`` its business key so surrogate keys
    are deterministic for an identical Silver snapshot.
    """

    def slv(table: str) -> str:
        return qualified(silver_schema, table)

    def wh(table: str) -> str:
        return qualified(warehouse_schema, table)

    statements: list[tuple[str, str]] = [
        (
            "dim_region",
            f'INSERT INTO {wh("dim_region")} ("region_id", "region_name", "demand_weight") '
            f'SELECT "region_id", "region_name", "demand_weight" '
            f'FROM {slv("regions")} ORDER BY "region_id"',
        ),
        (
            "dim_canal",
            f"INSERT INTO {wh('dim_canal')} "
            '("channel_id", "channel_name", "channel_type", "demand_weight") '
            f'SELECT "channel_id", "channel_name", "channel_type", "demand_weight" '
            f'FROM {slv("channels")} ORDER BY "channel_id"',
        ),
        (
            "dim_date",
            f"INSERT INTO {wh('dim_date')} "
            '("date", "day", "year", "month", "quarter", "iso_week", "weekday", '
            '"weekday_name", "is_weekend", "is_holiday", "holiday_name", '
            '"is_ramadan", "is_eid_al_fitr", "is_eid_al_adha", "season") '
            'SELECT "date", EXTRACT(DAY FROM "date")::BIGINT, "year", "month", '
            '"quarter", "iso_week", "weekday", "weekday_name", "is_weekend", '
            '"is_holiday", "holiday_name", "is_ramadan", "is_eid_al_fitr", '
            f'"is_eid_al_adha", "season" FROM {slv("calendar")} ORDER BY "date"',
        ),
        (
            "dim_magasin",
            f"INSERT INTO {wh('dim_magasin')} "
            '("store_id", "store_name", "city", "region_id", "region_sk", '
            '"store_format", "size_sqm", "open_date", "is_active") '
            'SELECT s."store_id", s."store_name", s."city", s."region_id", '
            'r."region_sk", s."store_format", s."size_sqm", s."open_date", '
            's."is_active" '
            f"FROM {slv('stores')} s JOIN {wh('dim_region')} r "
            'ON r."region_id" = s."region_id" ORDER BY s."store_id"',
        ),
        (
            "dim_client",
            f"INSERT INTO {wh('dim_client')} "
            '("customer_id", "signup_date", "region_id", "region_sk", "birth_year", '
            '"gender", "segment", "loyalty_tier", "preferred_channel_id", '
            '"canal_sk", "is_active") '
            'SELECT c."customer_id", c."signup_date", c."region_id", r."region_sk", '
            'c."birth_year", c."gender", c."segment", c."loyalty_tier", '
            'c."preferred_channel_id", ch."canal_sk", c."is_active" '
            f"FROM {slv('customers')} c "
            f'JOIN {wh("dim_region")} r ON r."region_id" = c."region_id" '
            f'JOIN {wh("dim_canal")} ch ON ch."channel_id" = c."preferred_channel_id" '
            'ORDER BY c."customer_id"',
        ),
        (
            "dim_produit",
            f"INSERT INTO {wh('dim_produit')} "
            '("product_id", "product_name", "category", "subcategory", "brand", '
            '"unit_cost_mad", "unit_price_mad", "margin_pct", "launch_date", '
            '"is_active") '
            'SELECT "product_id", "product_name", "category", "subcategory", '
            '"brand", "unit_cost_mad", "unit_price_mad", "margin_pct", '
            f'"launch_date", "is_active" FROM {slv("products")} '
            'ORDER BY "product_id"',
        ),
        (
            "dim_promotion",
            f"INSERT INTO {wh('dim_promotion')} "
            '("promotion_id", "product_id", "produit_sk", "region_id", "region_sk", '
            '"channel_id", "canal_sk", "promotion_type", "discount_pct", '
            '"start_date", "end_date") '
            'SELECT p."promotion_id", p."product_id", pr."produit_sk", '
            'p."region_id", r."region_sk", p."channel_id", c."canal_sk", '
            'p."promotion_type", p."discount_pct", p."start_date", p."end_date" '
            f"FROM {slv('promotions')} p "
            f'JOIN {wh("dim_produit")} pr ON pr."product_id" = p."product_id" '
            f'LEFT JOIN {wh("dim_region")} r ON r."region_id" = p."region_id" '
            f'LEFT JOIN {wh("dim_canal")} c ON c."channel_id" = p."channel_id" '
            'ORDER BY p."promotion_id"',
        ),
        (
            "fact_ventes",
            f"INSERT INTO {wh('fact_ventes')} "
            '("date_sk", "magasin_sk", "canal_sk", "client_sk", "produit_sk", '
            '"promotion_sk", "sale_id", "transaction_id", "hour_of_day", '
            '"quantity", "unit_price_mad", "discount_pct", "net_amount_mad", '
            '"cost_amount_mad", "margin_mad") '
            'SELECT d."date_sk", m."magasin_sk", ca."canal_sk", cl."client_sk", '
            'pr."produit_sk", pm."promotion_sk", s."sale_id", s."transaction_id", '
            's."hour_of_day", s."quantity", s."unit_price_mad", s."discount_pct", '
            's."net_amount_mad", s."cost_amount_mad", s."margin_mad" '
            f"FROM {slv('sales')} s "
            f'JOIN {wh("dim_date")} d ON d."date" = s."sale_date" '
            f'JOIN {wh("dim_magasin")} m ON m."store_id" = s."store_id" '
            f'JOIN {wh("dim_canal")} ca ON ca."channel_id" = s."channel_id" '
            f'JOIN {wh("dim_produit")} pr ON pr."product_id" = s."product_id" '
            f'LEFT JOIN {wh("dim_client")} cl ON cl."customer_id" = s."customer_id" '
            f"LEFT JOIN {wh('dim_promotion')} pm "
            'ON pm."promotion_id" = s."promotion_id" ORDER BY s."sale_id"',
        ),
        (
            "fact_achats",
            f"INSERT INTO {wh('fact_achats')} "
            '("date_sk", "magasin_sk", "produit_sk", "purchase_id", "quantity", '
            '"unit_cost_mad", "total_cost_mad") '
            'SELECT d."date_sk", m."magasin_sk", p."produit_sk", s."purchase_id", '
            's."quantity", s."unit_cost_mad", s."total_cost_mad" '
            f"FROM {slv('purchases')} s "
            f'JOIN {wh("dim_date")} d ON d."date" = s."purchase_date" '
            f'JOIN {wh("dim_magasin")} m ON m."store_id" = s."store_id" '
            f'JOIN {wh("dim_produit")} p ON p."product_id" = s."product_id" '
            'ORDER BY s."purchase_id"',
        ),
        (
            "fact_stock",
            f"INSERT INTO {wh('fact_stock')} "
            '("date_sk", "magasin_sk", "produit_sk", "quantity_on_hand", '
            '"stock_value_mad", "stock_status") '
            'SELECT d."date_sk", m."magasin_sk", p."produit_sk", '
            's."quantity_on_hand", s."stock_value_mad", s."stock_status" '
            f"FROM {slv('inventory')} s "
            f'JOIN {wh("dim_date")} d ON d."date" = s."snapshot_date" '
            f'JOIN {wh("dim_magasin")} m ON m."store_id" = s."store_id" '
            f'JOIN {wh("dim_produit")} p ON p."product_id" = s."product_id" '
            'ORDER BY d."date_sk", m."magasin_sk", p."produit_sk"',
        ),
    ]
    expected = (*DIMENSION_ORDER, *FACT_ORDER)
    actual = tuple(name for name, _ in statements)
    if actual != expected:
        raise WarehouseError(f"load order mismatch: {actual} != {expected}")
    return statements


__all__ = [
    "DERIVED_COLUMNS",
    "DIMENSION_ORDER",
    "DIMENSIONS",
    "FACT_ORDER",
    "FACTS",
    "RECONCILIATION_MEASURES",
    "TABLES",
    "TABLE_BY_NAME",
    "Column",
    "ForeignKey",
    "MeasureMetric",
    "WarehouseTable",
    "ident",
    "qualified",
    "render_load_statements",
    "render_truncate_statement",
    "render_warehouse_ddl",
]
