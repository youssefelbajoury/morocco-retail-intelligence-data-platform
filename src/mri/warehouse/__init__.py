"""Star-schema warehouse: seven dimensions and three facts built from Silver.

The package renders the ``warehouse`` schema DDL, reloads every table from the
Silver layer in dependency order and reconciles row counts and measure sums
against their Silver sources.
"""

from __future__ import annotations

from mri.warehouse.loader import (
    MeasureCheck,
    ReconciliationEntry,
    WarehouseBuilder,
    WarehouseBuildResult,
)
from mri.warehouse.schema import (
    DIMENSIONS,
    FACTS,
    TABLES,
    WarehouseTable,
    render_load_statements,
    render_truncate_statement,
    render_warehouse_ddl,
)

__all__ = [
    "DIMENSIONS",
    "FACTS",
    "TABLES",
    "MeasureCheck",
    "ReconciliationEntry",
    "WarehouseBuildResult",
    "WarehouseBuilder",
    "WarehouseTable",
    "render_load_statements",
    "render_truncate_statement",
    "render_warehouse_ddl",
]
