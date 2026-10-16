"""Assemble the final lineage graph from declared and observed evidence.

The builder merges design-time and runtime lineage, promotes dbt edges whose
child node executed in the observed run to ``inferred``, upgrades edges that the
run named on both endpoints to ``observed``, and reports datasets that are
referenced but have no producer as ``unavailable``.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from mri.ingestion.config import Settings
from mri.lineage.declared import (
    add_dbt_graph,
    add_silver_routing,
    add_superset_graph,
    add_warehouse_graph,
)
from mri.lineage.model import (
    KIND_DECLARED,
    KIND_INFERRED,
    KIND_UNAVAILABLE,
    LAYER_SOURCE,
    Dataset,
    Edge,
    LineageGraph,
)
from mri.lineage.observed import add_observed

DEFAULT_MANIFEST = Path("dbt/target/manifest.json")
DEFAULT_RUN_RESULTS = Path("dbt/target/run_results.json")
DEFAULT_SEED = Path("dashboards/superset/seed.py")


def mark_unavailable(graph: LineageGraph) -> int:
    """Flag referenced-but-unproduced datasets as lineage ``unavailable``."""
    produced = {edge.downstream for edge in graph.edges.values()}
    unknown_dataset = Dataset(namespace="unknown", name="unresolved", layer=LAYER_SOURCE)
    graph.add_dataset(unknown_dataset)
    marked = 0
    for dataset_id, dataset in sorted(graph.datasets.items()):
        if dataset.layer == LAYER_SOURCE or dataset_id in produced:
            continue
        referenced = any(edge.upstream == dataset_id for edge in graph.edges.values())
        if not referenced:
            continue
        graph.add_edge(
            Edge(
                upstream=unknown_dataset.id,
                downstream=dataset_id,
                job="unknown",
                kind=KIND_UNAVAILABLE,
                evidence="no producing edge in declared or observed lineage",
            )
        )
        marked += 1
    return marked


def promote_observed(graph: LineageGraph, executed_nodes: set[str]) -> int:
    """Upgrade dbt edges whose child model ran to ``inferred``."""
    promoted = 0
    for key, edge in list(graph.edges.items()):
        if edge.kind == KIND_DECLARED and edge.node in executed_nodes:
            graph.edges[key] = replace(
                edge,
                kind=KIND_INFERRED,
                evidence=f"{edge.evidence}; child ran (run) {edge.node}",
            )
            promoted += 1
    return promoted


def build_lineage(
    settings: Settings,
    *,
    dag_run_id: str = "",
    dbt_manifest: Path = DEFAULT_MANIFEST,
    dbt_run_results: Path = DEFAULT_RUN_RESULTS,
    superset_seed: Path = DEFAULT_SEED,
    events_dir: Path | None = None,
    ingestion_manifest_dir: Path | None = None,
    bronze_manifest_dir: Path | None = None,
) -> LineageGraph:
    """Build the merged graph for one optional dag run."""
    graph = LineageGraph(dag_run_id=dag_run_id)
    add_silver_routing(graph)
    add_dbt_graph(graph, dbt_manifest)
    add_warehouse_graph(
        graph,
        warehouse_schema=settings.warehouse_schema,
        silver_schema=settings.silver_schema,
    )
    add_superset_graph(graph, superset_seed)
    evidence = add_observed(
        graph,
        events_dir=events_dir or settings.observability_dir,
        ingestion_manifest_dir=ingestion_manifest_dir or settings.manifest_dir,
        bronze_manifest_dir=bronze_manifest_dir or settings.bronze_manifest_dir,
        bucket_raw=settings.bucket_raw,
        bucket_bronze=settings.bucket_bronze,
        silver_schema=settings.silver_schema,
        warehouse_schema=settings.warehouse_schema,
        dag_run_id=dag_run_id,
        run_results_path=dbt_run_results,
    )
    promote_observed(graph, evidence.executed_dbt_nodes)
    mark_unavailable(graph)
    return graph


__all__ = ["build_lineage", "mark_unavailable", "promote_observed"]
