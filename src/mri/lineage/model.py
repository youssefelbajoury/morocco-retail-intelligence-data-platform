"""Core lineage model for the Morocco Retail Intelligence platform.

Lineage is represented as a directed graph of :class:`Dataset` nodes joined by
:class:`Edge` transitions produced by a named job. Every edge is tagged with how
it was established:

``observed``
    Both endpoints and the job were named by a real pipeline run (audit events
    or run manifests). This is the strongest evidence.
``inferred``
    The downstream dataset was produced by an observed run, but the upstream was
    resolved from a declared mapping (for example a dbt ``parent_map``).
``declared``
    The edge exists in source-controlled configuration (dbt project, warehouse
    load SQL, Superset seed, Silver source routing) but was not exercised by the
    observed run.
``unavailable``
    A dataset is referenced as an input but no producing edge can be found.

The distinction is deliberate: the platform never claims runtime lineage when
only a design-time declaration exists.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

KIND_OBSERVED = "observed"
KIND_INFERRED = "inferred"
KIND_DECLARED = "declared"
KIND_UNAVAILABLE = "unavailable"

KINDS: tuple[str, ...] = (KIND_OBSERVED, KIND_INFERRED, KIND_DECLARED, KIND_UNAVAILABLE)
_KIND_RANK: dict[str, int] = {
    KIND_UNAVAILABLE: 0,
    KIND_DECLARED: 1,
    KIND_INFERRED: 2,
    KIND_OBSERVED: 3,
}

LAYER_SOURCE = "source"
LAYER_RAW = "raw"
LAYER_BRONZE = "bronze"
LAYER_SILVER = "silver"
LAYER_DBT = "dbt"
LAYER_WAREHOUSE = "warehouse"
LAYER_BI = "bi"

LAYER_ORDER: tuple[str, ...] = (
    LAYER_SOURCE,
    LAYER_RAW,
    LAYER_BRONZE,
    LAYER_SILVER,
    LAYER_DBT,
    LAYER_WAREHOUSE,
    LAYER_BI,
)


@dataclass(frozen=True)
class Dataset:
    """One addressable dataset: a file, object, table, BI dataset or dashboard."""

    namespace: str
    name: str
    layer: str

    @property
    def id(self) -> str:
        return f"{self.namespace}:{self.name}"

    def as_dict(self) -> dict[str, str]:
        return {
            "id": self.id,
            "namespace": self.namespace,
            "name": self.name,
            "layer": self.layer,
        }


@dataclass(frozen=True)
class Edge:
    """A directed transition between two datasets produced by ``job``."""

    upstream: str
    downstream: str
    job: str
    kind: str
    run_id: str = ""
    node: str = ""
    evidence: str = ""

    @property
    def key(self) -> tuple[str, str, str]:
        return (self.upstream, self.downstream, self.job)

    def as_dict(self) -> dict[str, str]:
        return {
            "upstream": self.upstream,
            "downstream": self.downstream,
            "job": self.job,
            "kind": self.kind,
            "run_id": self.run_id,
            "node": self.node,
            "evidence": self.evidence,
        }


@dataclass
class LineageGraph:
    """Accumulator for datasets, edges and the runs that were observed."""

    dag_run_id: str = ""
    datasets: dict[str, Dataset] = field(default_factory=dict)
    edges: dict[tuple[str, str, str], Edge] = field(default_factory=dict)
    runs: list[dict[str, Any]] = field(default_factory=list)

    def add_dataset(self, dataset: Dataset) -> None:
        self.datasets.setdefault(dataset.id, dataset)

    def add_edge(self, edge: Edge) -> None:
        """Keep the strongest evidence seen for one ``(upstream, downstream, job)``."""
        existing = self.edges.get(edge.key)
        if existing is None or _KIND_RANK[edge.kind] > _KIND_RANK[existing.kind]:
            self.edges[edge.key] = edge

    def has_edge(self, upstream: str, downstream: str, job: str) -> bool:
        return (upstream, downstream, job) in self.edges

    def edge_list(self) -> list[Edge]:
        return sorted(
            self.edges.values(),
            key=lambda e: (e.upstream, e.downstream, e.job),
        )

    def layer_counts(self) -> dict[str, int]:
        counts = {layer: 0 for layer in LAYER_ORDER}
        for dataset in self.datasets.values():
            counts[dataset.layer] = counts.get(dataset.layer, 0) + 1
        return counts

    def kind_counts(self) -> dict[str, int]:
        counts = {kind: 0 for kind in KINDS}
        for edge in self.edges.values():
            counts[edge.kind] = counts.get(edge.kind, 0) + 1
        return counts

    def summary(self) -> dict[str, Any]:
        return {
            "dag_run_id": self.dag_run_id,
            "datasets": len(self.datasets),
            "edges": len(self.edges),
            "edges_by_kind": self.kind_counts(),
            "datasets_by_layer": self.layer_counts(),
            "runs": len(self.runs),
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "dag_run_id": self.dag_run_id,
            "summary": self.summary(),
            "datasets": [self.datasets[key].as_dict() for key in sorted(self.datasets)],
            "edges": [edge.as_dict() for edge in self.edge_list()],
            "runs": self.runs,
        }


def file_dataset(name: str) -> Dataset:
    return Dataset(namespace="file", name=name, layer=LAYER_SOURCE)


def api_dataset(source: str) -> Dataset:
    return Dataset(namespace="api", name=source, layer=LAYER_SOURCE)


def object_dataset(bucket: str, key: str, layer: str) -> Dataset:
    return Dataset(namespace="minio", name=f"{bucket}/{key}", layer=layer)


def table_dataset(schema: str, table: str, layer: str) -> Dataset:
    return Dataset(namespace="postgres", name=f"{schema}.{table}", layer=layer)


def superset_dataset(kind: str, name: str) -> Dataset:
    return Dataset(namespace="superset", name=f"{kind}.{name}", layer=LAYER_BI)


__all__ = [
    "KINDS",
    "KIND_DECLARED",
    "KIND_INFERRED",
    "KIND_OBSERVED",
    "KIND_UNAVAILABLE",
    "LAYER_BI",
    "LAYER_BRONZE",
    "LAYER_DBT",
    "LAYER_ORDER",
    "LAYER_RAW",
    "LAYER_SILVER",
    "LAYER_SOURCE",
    "LAYER_WAREHOUSE",
    "Dataset",
    "Edge",
    "LineageGraph",
    "api_dataset",
    "file_dataset",
    "object_dataset",
    "superset_dataset",
    "table_dataset",
]
