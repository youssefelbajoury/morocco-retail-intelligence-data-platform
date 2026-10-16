"""Runtime (observed) lineage reconstructed from a real pipeline run.

Observed edges are only produced when the run itself names both endpoints. The
audit event log supplies run/job association; the ingestion and Bronze manifests
supply the concrete object keys that a file/batch was written to. Nothing is
guessed: a missing manifest simply yields fewer observed edges.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mri.lineage.model import (
    KIND_OBSERVED,
    LAYER_BRONZE,
    LAYER_RAW,
    LAYER_SILVER,
    LAYER_WAREHOUSE,
    Edge,
    LineageGraph,
    api_dataset,
    file_dataset,
    object_dataset,
    table_dataset,
)
from mri.observability.events import STATUS_FAILED, STATUS_SUCCESS, PipelineEvent
from mri.observability.store import EventLog

_INGEST_OK = ("ingested", "already_ingested")
_BRONZE_OK = ("bronzed", "already_bronzed")


@dataclass
class ObservedEvidence:
    """What the observed run actually touched."""

    dag_run_id: str = ""
    run_ids: list[str] = field(default_factory=list)
    executed_dbt_nodes: set[str] = field(default_factory=set)


def _read_manifest(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def _record_runs(graph: LineageGraph, events: list[PipelineEvent]) -> None:
    grouped: dict[str, dict[str, Any]] = {}
    for event in events:
        entry = grouped.setdefault(
            event.run_id,
            {
                "run_id": event.run_id,
                "dag_run_id": event.dag_run_id,
                "pipeline": event.pipeline,
                "stage": event.stage,
                "status": STATUS_SUCCESS,
                "started_at": event.timestamp,
                "finished_at": event.timestamp,
                "events": 0,
                "records_read": 0,
                "records_written": 0,
                "records_rejected": 0,
            },
        )
        entry["events"] += 1
        entry["finished_at"] = event.timestamp
        if event.status == STATUS_FAILED:
            entry["status"] = STATUS_FAILED
        for field_name in ("records_read", "records_written", "records_rejected"):
            value = getattr(event, field_name)
            if isinstance(value, int):
                entry[field_name] = max(int(entry[field_name]), value)
    graph.runs = sorted(grouped.values(), key=lambda item: (item["pipeline"], item["run_id"]))


def add_observed(
    graph: LineageGraph,
    *,
    events_dir: Path,
    ingestion_manifest_dir: Path,
    bronze_manifest_dir: Path,
    bucket_raw: str,
    bucket_bronze: str,
    silver_schema: str,
    warehouse_schema: str,
    dag_run_id: str = "",
    run_results_path: Path | None = None,
) -> ObservedEvidence:
    """Add every edge named by the run; return the evidence gathered."""
    events = EventLog.resolve(events_dir).read_events()
    if dag_run_id:
        events = [event for event in events if event.dag_run_id == dag_run_id]
    _record_runs(graph, events)

    evidence = ObservedEvidence(dag_run_id=dag_run_id)
    evidence.run_ids = sorted({event.run_id for event in events if event.run_id})

    _add_ingestion(graph, events, ingestion_manifest_dir, bucket_raw, evidence, dag_run_id)
    _add_external(graph, events, bucket_raw)
    _add_bronze(graph, events, bronze_manifest_dir, bucket_raw, bucket_bronze)
    _add_silver(graph, events, bucket_bronze, silver_schema)
    _add_warehouse(graph, events, silver_schema, warehouse_schema)
    _add_dbt(graph, events, run_results_path, evidence)
    return evidence


def _add_ingestion(
    graph: LineageGraph,
    events: list[PipelineEvent],
    manifest_dir: Path,
    bucket_raw: str,
    evidence: ObservedEvidence,
    dag_run_id: str,
) -> None:
    run_ids = sorted(
        {
            event.run_id
            for event in events
            if event.pipeline == "ingestion" and event.stage == "ingest"
        }
    )
    for run_id in run_ids:
        manifest = _read_manifest(manifest_dir / f"{run_id}.json")
        if manifest is None:
            continue
        for record in manifest.get("records", []):
            if not isinstance(record, dict):
                continue
            object_key = record.get("object_key")
            if record.get("outcome") not in _INGEST_OK or not object_key:
                continue
            upstream = file_dataset(str(record.get("original_filename") or record.get("source")))
            downstream = object_dataset(bucket_raw, str(object_key), LAYER_RAW)
            graph.add_dataset(upstream)
            graph.add_dataset(downstream)
            graph.add_edge(
                Edge(
                    upstream=upstream.id,
                    downstream=downstream.id,
                    job="ingest",
                    kind=KIND_OBSERVED,
                    run_id=run_id,
                    evidence=f"ingestion manifest sha256={record.get('sha256')}",
                )
            )


def _add_external(graph: LineageGraph, events: list[PipelineEvent], bucket_raw: str) -> None:
    for event in events:
        if event.pipeline != "ingestion" or event.stage != "external":
            continue
        if event.status in ("failed", "skipped"):
            continue
        object_key = event.payload.get("object_key")
        if not object_key:
            continue
        upstream = api_dataset(event.source)
        downstream = object_dataset(bucket_raw, str(object_key), LAYER_RAW)
        graph.add_dataset(upstream)
        graph.add_dataset(downstream)
        graph.add_edge(
            Edge(
                upstream=upstream.id,
                downstream=downstream.id,
                job="external",
                kind=KIND_OBSERVED,
                run_id=event.run_id,
                evidence="ingestion external event",
            )
        )


def _add_bronze(
    graph: LineageGraph,
    events: list[PipelineEvent],
    manifest_dir: Path,
    bucket_raw: str,
    bucket_bronze: str,
) -> None:
    run_ids = sorted(
        {event.run_id for event in events if event.pipeline == "bronze" and event.stage == "bronze"}
    )
    for run_id in run_ids:
        manifest = _read_manifest(manifest_dir / f"{run_id}.json")
        if manifest is None:
            continue
        for record in manifest.get("records", []):
            if not isinstance(record, dict):
                continue
            if record.get("outcome") not in _BRONZE_OK:
                continue
            raw_key = str(record.get("raw_key") or "")
            part_keys = record.get("part_keys") or []
            if not raw_key or not part_keys:
                continue
            upstream = object_dataset(bucket_raw, raw_key, LAYER_RAW)
            graph.add_dataset(upstream)
            promoted_key = record.get("promoted_key")
            products = [str(part) for part in (part_keys or [])]
            if promoted_key:
                products.append(str(promoted_key))
            for product in products:
                downstream = object_dataset(bucket_bronze, product, LAYER_BRONZE)
                graph.add_dataset(downstream)
                graph.add_edge(
                    Edge(
                        upstream=upstream.id,
                        downstream=downstream.id,
                        job="bronze",
                        kind=KIND_OBSERVED,
                        run_id=run_id,
                        evidence="bronze manifest",
                    )
                )


def _add_silver(
    graph: LineageGraph,
    events: list[PipelineEvent],
    bucket_bronze: str,
    silver_schema: str,
) -> None:
    for event in events:
        if event.pipeline != "silver" or event.stage != "silver":
            continue
        if event.status != STATUS_SUCCESS:
            continue
        target_table = event.payload.get("target_table")
        marker_key = event.payload.get("marker_key")
        if not target_table or not marker_key:
            continue
        upstream = object_dataset(bucket_bronze, str(marker_key), LAYER_BRONZE)
        downstream = table_dataset(silver_schema, str(target_table), LAYER_SILVER)
        graph.add_dataset(upstream)
        graph.add_dataset(downstream)
        graph.add_edge(
            Edge(
                upstream=upstream.id,
                downstream=downstream.id,
                job="silver.load",
                kind=KIND_OBSERVED,
                run_id=event.run_id,
                evidence="silver event marker_key",
            )
        )


def _add_warehouse(
    graph: LineageGraph,
    events: list[PipelineEvent],
    silver_schema: str,
    warehouse_schema: str,
) -> None:
    for event in events:
        if event.pipeline != "warehouse" or event.stage != "warehouse":
            continue
        if event.status != STATUS_SUCCESS:
            continue
        if not event.source or not event.batch_id:
            continue
        upstream = table_dataset(silver_schema, event.source, LAYER_SILVER)
        downstream = table_dataset(warehouse_schema, event.batch_id, LAYER_WAREHOUSE)
        graph.add_dataset(upstream)
        graph.add_dataset(downstream)
        graph.add_edge(
            Edge(
                upstream=upstream.id,
                downstream=downstream.id,
                job="warehouse.load",
                kind=KIND_OBSERVED,
                run_id=event.run_id,
                evidence="warehouse reconciliation event",
            )
        )


_DBT_OK = ("success", "pass")


def _add_dbt(
    graph: LineageGraph,
    events: list[PipelineEvent],
    run_results_path: Path | None,
    evidence: ObservedEvidence,
) -> None:
    for event in events:
        if event.pipeline != "dbt" or event.stage != "dbt":
            continue
        node = event.payload.get("node")
        if node and event.status in _DBT_OK:
            evidence.executed_dbt_nodes.add(str(node))
    if run_results_path and run_results_path.is_file():
        payload = _read_manifest(run_results_path)
        if payload is not None:
            for result in payload.get("results", []):
                if not isinstance(result, dict):
                    continue
                if result.get("status") in _DBT_OK and result.get("unique_id"):
                    evidence.executed_dbt_nodes.add(str(result["unique_id"]))


__all__ = ["ObservedEvidence", "add_observed"]
