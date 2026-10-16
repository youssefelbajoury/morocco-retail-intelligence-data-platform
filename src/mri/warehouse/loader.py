"""Build the star-schema warehouse from the Silver layer and reconcile it.

A build is one PostgreSQL transaction: create the schema if missing, create
every table/key/index (all ``IF NOT EXISTS``, so re-builds are idempotent),
``TRUNCATE ... RESTART IDENTITY CASCADE`` all tables, insert dimensions in
dependency order (``ORDER BY`` business key, which makes surrogate keys
deterministic per Silver snapshot), insert the three facts, then reconcile:

* one row-count entry per table (Silver source rows vs warehouse rows);
* for every fact, one measure entry per reconciled aggregate (sums compared
  as exact integers or ``round(sum(x::numeric), 2)`` decimals, so the
  comparison never depends on float summation order).

Any connection or SQL failure raises :class:`WarehouseError` and rolls the
whole build back, leaving the warehouse exactly as it was.
"""

from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from typing import Any

import psycopg

from mri.ingestion.config import Settings
from mri.ingestion.exceptions import WarehouseError
from mri.ingestion.manifest import new_run_id, now_iso
from mri.silver import conninfo_for
from mri.warehouse.schema import (
    RECONCILIATION_MEASURES,
    TABLES,
    MeasureMetric,
    qualified,
    render_load_statements,
    render_truncate_statement,
    render_warehouse_ddl,
)


@dataclass(frozen=True)
class MeasureCheck:
    """Difference of one aggregated measure between Silver and warehouse."""

    metric: str
    silver: float
    warehouse: float
    difference: float
    balanced: bool


@dataclass(frozen=True)
class ReconciliationEntry:
    """Reconciliation outcome for one warehouse table."""

    target_table: str
    source_table: str
    silver_rows: int
    warehouse_rows: int
    measures: tuple[MeasureCheck, ...]

    @property
    def balanced(self) -> bool:
        if self.silver_rows != self.warehouse_rows:
            return False
        return all(measure.balanced for measure in self.measures)


@dataclass(frozen=True)
class WarehouseBuildResult:
    """Aggregate outcome of one warehouse build."""

    run_id: str
    started_at: str
    finished_at: str
    schemas: dict[str, str]
    tables: dict[str, int]
    reconciliation: tuple[ReconciliationEntry, ...]
    timing: dict[str, float]

    @property
    def ok(self) -> bool:
        return all(entry.balanced for entry in self.reconciliation)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "run_id": self.run_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "schemas": dict(self.schemas),
            "tables": dict(self.tables),
            "balanced": self.ok,
            "timing": dict(self.timing),
            "reconciliation": [],
        }
        for entry in self.reconciliation:
            item = asdict(entry)
            item["balanced"] = entry.balanced
            payload["reconciliation"].append(item)
        return payload


def _aggregate_expression(metric: MeasureMetric) -> str:
    column = f'"{metric.column}"'
    if metric.money:
        return f"round(COALESCE(sum({column}::numeric), 0), 2)"
    return f"COALESCE(sum({column}), 0)"


class WarehouseBuilder:
    """Rebuilds the warehouse schema from the configured Silver schema."""

    def __init__(
        self,
        settings: Settings,
        *,
        warehouse_schema: str | None = None,
        silver_schema: str | None = None,
    ) -> None:
        self._settings = settings
        self._warehouse_schema = warehouse_schema or settings.warehouse_schema
        self._silver_schema = silver_schema or settings.silver_schema

    @property
    def warehouse_schema(self) -> str:
        return self._warehouse_schema

    @property
    def silver_schema(self) -> str:
        return self._silver_schema

    def build(self, run_id: str | None = None) -> WarehouseBuildResult:
        """Run one full rebuild plus reconciliation; raise on any SQL error."""
        run_id = run_id or new_run_id()
        started_at = now_iso()
        run_clock = time.perf_counter()
        timing: dict[str, float] = {
            "ddl_seconds": 0.0,
            "load_seconds": 0.0,
            "reconcile_seconds": 0.0,
        }
        try:
            connection = psycopg.connect(conninfo_for(self._settings))
        except psycopg.Error as exc:
            raise WarehouseError(f"cannot connect to PostgreSQL: {exc}") from exc
        try:
            with connection as conn:
                clock = time.perf_counter()
                self._ensure_schema(conn)
                timing["ddl_seconds"] = round(time.perf_counter() - clock, 3)
                clock = time.perf_counter()
                tables = self._load_all(conn)
                timing["load_seconds"] = round(time.perf_counter() - clock, 3)
                clock = time.perf_counter()
                reconciliation = self._reconcile(conn)
                timing["reconcile_seconds"] = round(time.perf_counter() - clock, 3)
        except psycopg.Error as exc:
            raise WarehouseError(f"PostgreSQL error: {exc}") from exc
        timing["total_seconds"] = round(time.perf_counter() - run_clock, 3)
        return WarehouseBuildResult(
            run_id=run_id,
            started_at=started_at,
            finished_at=now_iso(),
            schemas={"warehouse": self._warehouse_schema, "silver": self._silver_schema},
            tables=tables,
            reconciliation=tuple(reconciliation),
            timing=timing,
        )

    def _ensure_schema(self, conn: Any) -> None:
        statements = render_warehouse_ddl(
            warehouse_schema=self._warehouse_schema,
            silver_schema=self._silver_schema,
        )
        with conn.cursor() as cur:
            for statement in statements:
                cur.execute(statement)

    def _load_all(self, conn: Any) -> dict[str, int]:
        statements = render_load_statements(
            warehouse_schema=self._warehouse_schema,
            silver_schema=self._silver_schema,
        )
        truncate = render_truncate_statement(warehouse_schema=self._warehouse_schema)
        rows: dict[str, int] = {}
        with conn.cursor() as cur:
            cur.execute(truncate)
            for table, statement in statements:
                cur.execute(statement)
                rows[table] = int(cur.rowcount) if cur.rowcount >= 0 else -1
        return rows

    def _reconcile(self, conn: Any) -> list[ReconciliationEntry]:
        entries: list[ReconciliationEntry] = []
        for table in TABLES:
            metrics = RECONCILIATION_MEASURES.get(table.name, ())
            silver_rows, silver_values = self._aggregate(
                conn, self._silver_schema, table.source, metrics
            )
            warehouse_rows, warehouse_values = self._aggregate(
                conn, self._warehouse_schema, table.name, metrics
            )
            measures: list[MeasureCheck] = []
            for metric in metrics:
                silver = silver_values[metric.name]
                warehouse = warehouse_values[metric.name]
                measures.append(
                    MeasureCheck(
                        metric=metric.name,
                        silver=float(silver),
                        warehouse=float(warehouse),
                        difference=float(warehouse) - float(silver),
                        balanced=warehouse == silver,
                    )
                )
            entries.append(
                ReconciliationEntry(
                    target_table=table.name,
                    source_table=table.source,
                    silver_rows=silver_rows,
                    warehouse_rows=warehouse_rows,
                    measures=tuple(measures),
                )
            )
        return entries

    def _aggregate(
        self, conn: Any, schema: str, table: str, metrics: tuple[MeasureMetric, ...]
    ) -> tuple[int, dict[str, float]]:
        expressions = ["count(*)"]
        for metric in metrics:
            expressions.append(_aggregate_expression(metric))
        sql = f"SELECT {', '.join(expressions)} FROM {qualified(schema, table)}"
        row = conn.execute(sql).fetchone()
        values: dict[str, float] = {metric.name: 0.0 for metric in metrics}
        if row is None:
            return 0, values
        for index, metric in enumerate(metrics, start=1):
            values[metric.name] = float(row[index])
        return int(row[0]), values


__all__ = [
    "MeasureCheck",
    "ReconciliationEntry",
    "WarehouseBuildResult",
    "WarehouseBuilder",
]
