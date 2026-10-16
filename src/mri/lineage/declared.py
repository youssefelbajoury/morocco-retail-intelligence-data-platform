"""Design-time (declared) lineage extracted from source-controlled artifacts.

Nothing here is run-specific: declared lineage comes from the dbt manifest that
``dbt build`` produces, the warehouse load SQL, the Silver source routing table
and the Superset seed script. These edges are always available, even before a
single pipeline run.
"""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path
from typing import Any

from mri.lineage.model import (
    KIND_DECLARED,
    LAYER_DBT,
    LAYER_SILVER,
    LAYER_SOURCE,
    LAYER_WAREHOUSE,
    Dataset,
    Edge,
    LineageGraph,
    superset_dataset,
    table_dataset,
)
from mri.silver.ddl import SOURCE_TABLES
from mri.warehouse.schema import render_load_statements

_SEED_TABLE_RE = re.compile(r"\b(warehouse|audit)\.([A-Za-z_][A-Za-z0-9_]*)")


def source_dataset(source: str) -> Dataset:
    """A declared source *type* (contract id), distinct from concrete files."""
    return Dataset(namespace="source", name=source, layer=LAYER_SOURCE)


def add_silver_routing(graph: LineageGraph) -> int:
    """Declared ``source type -> silver table`` routing from ``SOURCE_TABLES``."""
    added = 0
    for source, table in sorted(SOURCE_TABLES.items()):
        graph.add_dataset(source_dataset(source))
        graph.add_dataset(table_dataset("silver", table, LAYER_SILVER))
        graph.add_edge(
            Edge(
                upstream=source_dataset(source).id,
                downstream=table_dataset("silver", table, LAYER_SILVER).id,
                job="silver.load",
                kind=KIND_DECLARED,
                evidence="src/mri/silver/ddl.py:SOURCE_TABLES",
            )
        )
        added += 1
    return added


def _dbt_node_dataset(node: dict[str, Any]) -> Dataset:
    resource_type = str(node.get("resource_type") or "")
    name = str(node.get("name") or "")
    if resource_type == "source":
        schema = str(node.get("source_name") or node.get("schema") or "silver")
        return table_dataset(schema, name, LAYER_SILVER)
    config = node.get("config")
    schema = ""
    if isinstance(config, dict):
        schema = str(config.get("schema") or "")
    schema = schema or str(node.get("schema") or "dbt")
    return table_dataset(schema, name, LAYER_DBT)


def add_dbt_graph(graph: LineageGraph, manifest_path: Path) -> int:
    """Declared dbt edges from ``manifest.json`` ``parent_map`` (models + snapshots)."""
    if not manifest_path.is_file():
        return 0
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    nodes: dict[str, Any] = dict(manifest.get("nodes") or {})
    sources: dict[str, Any] = dict(manifest.get("sources") or {})
    parent_map: dict[str, Any] = dict(manifest.get("parent_map") or {})
    by_id: dict[str, Any] = {**sources, **nodes}
    for node in by_id.values():
        graph.add_dataset(_dbt_node_dataset(node))
    added = 0
    for child_id, parents in parent_map.items():
        if child_id.startswith("test.") or child_id not in by_id:
            continue
        child = _dbt_node_dataset(by_id[child_id])
        for parent_id in parents:
            if parent_id.startswith("test.") or parent_id not in by_id:
                continue
            parent = _dbt_node_dataset(by_id[parent_id])
            graph.add_edge(
                Edge(
                    upstream=parent.id,
                    downstream=child.id,
                    job=f"dbt.{child.name.split('.', 1)[-1]}",
                    kind=KIND_DECLARED,
                    node=child_id,
                    evidence="dbt/target/manifest.json:parent_map",
                )
            )
            added += 1
    return added


def add_warehouse_graph(graph: LineageGraph, *, warehouse_schema: str, silver_schema: str) -> int:
    """Declared edges parsed from the warehouse load SQL (silver + dim joins)."""
    statements = render_load_statements(
        warehouse_schema=warehouse_schema, silver_schema=silver_schema
    )
    silver_re = re.compile(rf'FROM\s+"{re.escape(silver_schema)}"\."([A-Za-z_][A-Za-z0-9_]*)"')
    warehouse_re = re.compile(
        rf'(?:FROM|JOIN)\s+"{re.escape(warehouse_schema)}"\."([A-Za-z_][A-Za-z0-9_]*)"'
    )
    added = 0
    for table, sql in statements:
        graph.add_dataset(table_dataset(warehouse_schema, table, LAYER_WAREHOUSE))
        for match in silver_re.finditer(sql):
            graph.add_dataset(table_dataset(silver_schema, match.group(1), LAYER_SILVER))
            graph.add_edge(
                Edge(
                    upstream=table_dataset(silver_schema, match.group(1), LAYER_SILVER).id,
                    downstream=table_dataset(warehouse_schema, table, LAYER_WAREHOUSE).id,
                    job="warehouse.load",
                    kind=KIND_DECLARED,
                    evidence="src/mri/warehouse/schema.py:render_load_statements",
                )
            )
            added += 1
        for match in warehouse_re.finditer(sql):
            graph.add_dataset(table_dataset(warehouse_schema, match.group(1), LAYER_WAREHOUSE))
            graph.add_edge(
                Edge(
                    upstream=table_dataset(warehouse_schema, match.group(1), LAYER_WAREHOUSE).id,
                    downstream=table_dataset(warehouse_schema, table, LAYER_WAREHOUSE).id,
                    job="warehouse.load",
                    kind=KIND_DECLARED,
                    evidence="src/mri/warehouse/schema.py:render_load_statements",
                )
            )
            added += 1
    return added


def _string_value(node: ast.AST) -> str | None:
    """String constant, possibly wrapped in a method call like ``\"\".strip()``."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Constant)
        and isinstance(node.func.value.value, str)
        and node.func.attr in {"strip", "replace", "format"}
    ):
        return node.func.value.value
    return None


def _seed_strings_and_lists(
    module: ast.Module,
) -> tuple[dict[str, str], ast.List | None, ast.List | None]:
    strings: dict[str, str] = {}
    datasets_node: ast.List | None = None
    dashboards_node: ast.List | None = None
    for node in module.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if not isinstance(target, ast.Name):
            continue
        value = _string_value(node.value)
        if value is not None:
            strings[target.id] = value
        elif target.id == "DATASETS" and isinstance(node.value, ast.List):
            datasets_node = node.value
        elif target.id == "DASHBOARDS" and isinstance(node.value, ast.List):
            dashboards_node = node.value
    return strings, datasets_node, dashboards_node


def _superset_dataset_sql(
    datasets_node: ast.List | None, strings: dict[str, str]
) -> dict[str, str]:
    mapping: dict[str, str] = {}
    if datasets_node is None:
        return mapping
    for element in datasets_node.elts:
        if not isinstance(element, ast.Tuple) or len(element.elts) != 2:
            continue
        name_el, sql_el = element.elts
        if not (isinstance(name_el, ast.Constant) and isinstance(name_el.value, str)):
            continue
        name = name_el.value
        if isinstance(sql_el, ast.Name):
            mapping[name] = strings.get(sql_el.id, "")
        elif isinstance(sql_el, ast.Constant) and isinstance(sql_el.value, str):
            mapping[name] = sql_el.value
    return mapping


def _superset_dashboards(dashboards_node: ast.List | None) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    if dashboards_node is None:
        return pairs
    for element in dashboards_node.elts:
        if not isinstance(element, ast.Dict):
            continue
        title = dataset = ""
        for key, value in zip(element.keys, element.values, strict=False):
            if not (isinstance(key, ast.Constant) and isinstance(key.value, str)):
                continue
            if not (isinstance(value, ast.Constant) and isinstance(value.value, str)):
                continue
            if key.value == "title":
                title = value.value
            elif key.value == "dataset":
                dataset = value.value
        if title and dataset:
            pairs.append((dataset, title))
    return pairs


def add_superset_graph(graph: LineageGraph, seed_path: Path) -> int:
    """Declared Superset edges: warehouse/audit tables -> dataset -> dashboard."""
    if not seed_path.is_file():
        return 0
    module = ast.parse(seed_path.read_text(encoding="utf-8"))
    strings, datasets_node, dashboards_node = _seed_strings_and_lists(module)
    added = 0
    for ds_name, sql in sorted(_superset_dataset_sql(datasets_node, strings).items()):
        target = superset_dataset("dataset", ds_name)
        graph.add_dataset(target)
        for schema, table in sorted(set(_SEED_TABLE_RE.findall(sql))):
            graph.add_dataset(table_dataset(schema, table, LAYER_WAREHOUSE))
            graph.add_edge(
                Edge(
                    upstream=table_dataset(schema, table, LAYER_WAREHOUSE).id,
                    downstream=target.id,
                    job="superset.dataset",
                    kind=KIND_DECLARED,
                    evidence="dashboards/superset/seed.py:DATASETS",
                )
            )
            added += 1
    for ds_name, title in sorted(_superset_dashboards(dashboards_node)):
        dataset_node = superset_dataset("dataset", ds_name)
        dashboard_node = superset_dataset("dashboard", title)
        graph.add_dataset(dataset_node)
        graph.add_dataset(dashboard_node)
        graph.add_edge(
            Edge(
                upstream=dataset_node.id,
                downstream=dashboard_node.id,
                job="superset.dashboard",
                kind=KIND_DECLARED,
                evidence="dashboards/superset/seed.py:DASHBOARDS",
            )
        )
        added += 1
    return added


__all__ = [
    "add_dbt_graph",
    "add_silver_routing",
    "add_superset_graph",
    "add_warehouse_graph",
    "source_dataset",
]
