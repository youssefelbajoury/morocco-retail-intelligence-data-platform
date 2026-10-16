"""Data lineage: build and trace the platform's declared and observed graph.

The package answers "where did this data come from and where does it go?" for
every layer: source contracts, ingestion (file/API), Bronze, Silver, dbt,
the warehouse and Superset. Declared edges come from source-controlled
artifacts; observed edges come from the audit event log and run manifests.
"""

from mri.lineage.model import (
    KIND_DECLARED,
    KIND_INFERRED,
    KIND_OBSERVED,
    KIND_UNAVAILABLE,
    Dataset,
    Edge,
    LineageGraph,
)

__all__ = [
    "KIND_DECLARED",
    "KIND_INFERRED",
    "KIND_OBSERVED",
    "KIND_UNAVAILABLE",
    "Dataset",
    "Edge",
    "LineageGraph",
]
