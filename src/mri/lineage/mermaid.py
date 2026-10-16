"""Render the lineage graph as a Mermaid ``flowchart`` document.

Node labels are human-readable ``namespace/name`` pairs; edge labels carry the
producing job. Kind is visualised with the standard filled-node colours, so a
quick glance separates observed (green), inferred (yellow), declared (blue) and
unavailable (red) lineage.
"""

from __future__ import annotations

import re
from pathlib import Path

from mri.lineage.model import (
    KIND_DECLARED,
    KIND_INFERRED,
    KIND_OBSERVED,
    KIND_UNAVAILABLE,
    LAYER_ORDER,
    LineageGraph,
)

_COLORS: dict[str, str] = {
    KIND_OBSERVED: "#dff0d8",
    KIND_INFERRED: "#fcf8e3",
    KIND_DECLARED: "#d9edf7",
    KIND_UNAVAILABLE: "#f2dede",
}
_LAYER_LABELS: dict[str, str] = {
    "source": "Sources",
    "raw": "Raw objects (MinIO)",
    "bronze": "Bronze (MinIO)",
    "silver": "Silver (PostgreSQL)",
    "dbt": "dbt (PostgreSQL)",
    "warehouse": "Warehouse (PostgreSQL)",
    "bi": "Superset",
}


def _node_id(dataset_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]", "_", dataset_id)
    return f"n_{safe[:60]}"


def _label(dataset_id: str) -> str:
    namespace, _, name = dataset_id.partition(":")
    return f"{namespace}/{name}"


def render_mermaid(graph: LineageGraph) -> str:
    """Produce a deterministic Mermaid flowchart with per-layer subgraphs."""
    registry: dict[str, str] = {}
    by_layer_lines: dict[str, list[str]] = {layer: [] for layer in LAYER_ORDER}
    for dataset in graph.datasets.values():
        node = _node_id(dataset.id)
        registry[dataset.id] = node
        by_layer_lines[dataset.layer].append(f'    {node}["{_label(dataset.id)}"]')
    edge_lines: list[str] = []
    for edge in sorted(graph.edges.values(), key=lambda e: (e.upstream, e.downstream, e.job)):
        up_node = registry.get(edge.upstream)
        down_node = registry.get(edge.downstream)
        if up_node is None or down_node is None:
            continue
        label = edge.job.replace("dbt.mri.", "dbt.")
        edge_lines.append(f"    {up_node} -->|{label}| {down_node}")

    out = ["flowchart LR"]
    for layer in LAYER_ORDER:
        block = by_layer_lines.get(layer) or []
        label = _LAYER_LABELS.get(layer, layer)
        if not block:
            continue
        out.append(f"  subgraph {layer}[{label}]")
        out.extend(block)
        out.append("  end")
    out.extend(edge_lines)
    for kind, color in _COLORS.items():
        out.append(f"  classDef {kind} fill:{color};")
        for edge in graph.edges.values():
            if edge.kind != kind:
                continue
            target = registry.get(edge.downstream)
            if target:
                out.append(f"  class {target} {kind}")
    out.append("")
    return "\n".join(out)


def write_mermaid(path: Path, graph: LineageGraph) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_mermaid(graph), encoding="utf-8")
    return len(graph.edges)


__all__ = ["render_mermaid", "write_mermaid"]
