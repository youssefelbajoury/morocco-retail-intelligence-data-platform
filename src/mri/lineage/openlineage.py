"""OpenLineage-shaped RunEvent emission.

This is a *minimal*, spec-shaped subset of OpenLineage (fresh ``RunEvent``
records, one JSON line each) designed to be consumed by a future OpenLineage
backend such as Marquez. It intentionally depends on nothing new: the events are
written from the lineage graph that the platform already derives from its audit
log and dbt artifacts. A Marquez or other OL collection endpoint can be pointed
at ``reports/lineage/openlineage.jsonl`` later without re-instrumenting any
pipeline code. See ``docs/lineage/data-lineage.md`` for the evaluation.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mri.lineage.model import (
    KIND_INFERRED,
    KIND_OBSERVED,
    Dataset,
    LineageGraph,
)

PRODUCER = "https://github.com/anomalyco/morocco-retail-intelligence/blob/main/src/mri/lineage"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/definitions/RunEvent"
NAMESPACE_UUID = uuid.UUID("c4a60f20-2f3d-5a30-9c0e-1a2b3c4d5e6f")


def _uuid5(text: str) -> str:
    return str(uuid.uuid5(NAMESPACE_UUID, text))


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def ol_dataset(dataset: Dataset) -> dict[str, Any]:
    """Map a repo dataset onto OpenLineage namespace/name conventions."""
    namespace, name = dataset.namespace, dataset.name
    if namespace == "minio":
        bucket, _, key = name.partition("/")
        return {
            "namespace": f"minio://{bucket}",
            "name": key or name,
            "facets": {"dataSource": {"name": f"minio://{bucket}"}},
        }
    if namespace == "postgres":
        return {
            "namespace": "postgres://retail",
            "name": name,
            "facets": {"dataSource": {"name": "postgres://retail"}},
        }
    return {
        "namespace": namespace,
        "name": name,
        "facets": {"dataSource": {"name": namespace}},
    }


def _run_event(
    *,
    event_type: str,
    run_id: str,
    job_name: str,
    inputs: Iterable[Dataset],
    outputs: Iterable[Dataset],
    parent_run_id: str = "",
    parent_job_name: str = "",
    row_count: int | None = None,
) -> dict[str, Any]:
    run: dict[str, Any] = {"runId": run_id}
    if parent_run_id:
        run["facets"] = {
            "parent": {
                "run": {"runId": _uuid5(parent_run_id), "name": parent_job_name},
                "job": {"namespace": PRODUCER, "name": parent_job_name},
            }
        }
    job: dict[str, Any] = {
        "namespace": PRODUCER,
        "name": job_name,
        "facets": {"jobType": {"processingType": "BATCH", "integration": "mri"}},
    }
    output_facets: dict[str, Any] = {}
    if row_count is not None:
        output_facets["outputStatistics"] = {"rowCount": row_count}
    return {
        "_producer": PRODUCER,
        "_schemaURL": SCHEMA_URL,
        "eventTime": now_iso(),
        "eventType": event_type,
        "run": run,
        "job": job,
        "inputs": [ol_dataset(dataset) for dataset in inputs],
        "outputs": [{**ol_dataset(dataset), "facets": output_facets} for dataset in outputs],
    }


def iter_run_events(graph: LineageGraph) -> list[dict[str, Any]]:
    """One COMPLETE RunEvent per observed run, plus one per executed dbt node."""
    events: list[dict[str, Any]] = []
    by_run: dict[str, dict[str, Any]] = {}
    for edge in graph.edge_list():
        if edge.kind != KIND_OBSERVED or not edge.run_id:
            continue
        bucket = by_run.setdefault(
            edge.run_id,
            {"inputs": [], "outputs": [], "rows": 0, "job": edge.job},
        )
        bucket["inputs"].append(edge.upstream)
        bucket["outputs"].append(edge.downstream)
    runs_by_id = {run["run_id"]: run for run in graph.runs}
    for run_id, bucket in sorted(by_run.items()):
        run = runs_by_id.get(run_id, {})
        events.append(
            _run_event(
                event_type="COMPLETE",
                run_id=_uuid5(run_id),
                job_name=str(bucket["job"]),
                inputs=(graph.datasets[ds] for ds in bucket["inputs"]),
                outputs=(graph.datasets[ds] for ds in bucket["outputs"]),
                parent_run_id=str(run.get("dag_run_id") or run_id),
                parent_job_name=f"dag:{run.get('dag_run_id') or run_id}",
                row_count=int(run.get("records_written") or 0) or None,
            )
        )
    by_node: dict[str, dict[str, Any]] = {}
    for edge in graph.edge_list():
        if edge.kind != KIND_INFERRED or not edge.node:
            continue
        if edge.downstream not in by_node:
            by_node[edge.downstream] = {"inputs": [], "node": edge.node}
        by_node[edge.downstream]["inputs"].append(edge.upstream)
    for downstream, bucket in sorted(by_node.items()):
        node = str(bucket["node"])
        events.append(
            _run_event(
                event_type="COMPLETE",
                run_id=_uuid5(f"dbt:{node}"),
                job_name=f"dbt.{node.split('.', 2)[-1]}",
                inputs=(graph.datasets[ds] for ds in bucket["inputs"]),
                outputs=[graph.datasets[downstream]],
                parent_run_id=graph.dag_run_id or "manual",
                parent_job_name=f"dag:{graph.dag_run_id or 'manual'}",
            )
        )
    return events


def emit_openlineage(path: Path, events: Iterable[dict[str, Any]]) -> int:
    """Write Compact JSON-lines. Returns the number of events written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event, separators=(",", ":")) + "\n")
            written += 1
    return written


__all__ = ["SCHEMA_URL", "emit_openlineage", "iter_run_events"]
