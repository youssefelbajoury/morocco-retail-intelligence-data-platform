"""Unit tests for the data-lineage package.

No database, MinIO or dbt binary is required: declared lineage is parsed from
small synthetic artifacts and observed lineage is reconstructed from a synthetic
audit log plus run manifests. The suite pins the properties the phase demands —
truthful metadata, correct edges, stable run ids, failed/missing inputs, and the
absence of invented (false Cartesian) edges.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from mri.ingestion.config import Settings
from mri.lineage.build import build_lineage, mark_unavailable, promote_observed
from mri.lineage.cli import EXIT_FAILURE, EXIT_OK, main
from mri.lineage.declared import (
    add_dbt_graph,
    add_silver_routing,
    add_superset_graph,
    add_warehouse_graph,
)
from mri.lineage.mermaid import render_mermaid
from mri.lineage.model import (
    KIND_DECLARED,
    KIND_INFERRED,
    KIND_OBSERVED,
    KIND_UNAVAILABLE,
    LAYER_SILVER,
    LAYER_WAREHOUSE,
    Dataset,
    Edge,
    LineageGraph,
)
from mri.lineage.observed import add_observed
from mri.lineage.openlineage import SCHEMA_URL, iter_run_events
from mri.observability.events import PipelineEvent
from mri.observability.store import EventLog
from mri.silver.ddl import SOURCE_TABLES

DAG = "dag__lineage_test"


def _write(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _emit(
    events_dir: Path,
    *,
    run_id: str,
    pipeline: str,
    stage: str,
    status: str = "success",
    source: str = "",
    batch_id: str = "",
    records_read: int | None = None,
    records_written: int | None = None,
    payload: Mapping[str, Any] | None = None,
) -> None:
    event = PipelineEvent(
        timestamp="2026-01-01T00:00:00.000000Z",
        run_id=run_id,
        pipeline=pipeline,
        stage=stage,
        status=status,
        dag_run_id=DAG,
        source=source,
        batch_id=batch_id,
        records_read=records_read,
        records_written=records_written,
        payload=payload or {},
    )
    EventLog.resolve(events_dir).append(event)


# --------------------------------------------------------------------------- model


def test_graph_keeps_the_strongest_evidence_for_an_edge() -> None:
    graph = LineageGraph()
    graph.add_edge(Edge("a", "b", "job", KIND_DECLARED))
    graph.add_edge(Edge("a", "b", "job", KIND_OBSERVED))
    graph.add_edge(Edge("a", "b", "job", KIND_DECLARED))
    assert len(graph.edges) == 1
    assert graph.edge_list()[0].kind == KIND_OBSERVED
    assert graph.kind_counts()[KIND_OBSERVED] == 1


# ------------------------------------------------------------------------ declared


def test_silver_routing_is_declared_for_every_contract_source() -> None:
    graph = LineageGraph()
    count = add_silver_routing(graph)
    assert count == len(SOURCE_TABLES)
    edge = graph.edges[("source:sales", "postgres:silver.sales", "silver.load")]
    assert edge.kind == KIND_DECLARED
    # weather merges two sources into one table without inventing an edge per row.
    weather = [e for e in graph.edge_list() if e.downstream == "postgres:silver.weather"]
    assert {e.upstream for e in weather} == {
        "source:open_meteo_forecast",
        "source:open_meteo_history",
    }


def test_warehouse_declared_edges_come_from_load_sql() -> None:
    graph = LineageGraph()
    add_warehouse_graph(graph, warehouse_schema="warehouse", silver_schema="silver")
    assert graph.has_edge(
        "postgres:silver.sales", "postgres:warehouse.fact_ventes", "warehouse.load"
    )
    assert graph.has_edge(
        "postgres:silver.regions", "postgres:warehouse.dim_region", "warehouse.load"
    )
    # dimensions are joined, not copied: fact joins dim_date
    assert graph.has_edge(
        "postgres:warehouse.dim_date", "postgres:warehouse.fact_ventes", "warehouse.load"
    )


def test_dbt_declared_graph_maps_sources_and_skips_tests(tmp_path: Path) -> None:
    manifest = {
        "nodes": {
            "model.mri.stg_sales": {
                "resource_type": "model",
                "name": "stg_sales",
                "schema": "dbt_staging_x",
                "config": {"schema": "dbt_staging"},
            },
            "model.mri.fact_ventes": {
                "resource_type": "model",
                "name": "fact_ventes",
                "schema": "dbt_marts_x",
                "config": {"schema": "dbt_marts"},
            },
        },
        "sources": {
            "source.mri.silver.sales": {
                "resource_type": "source",
                "name": "sales",
                "source_name": "silver",
                "schema": "silver_x",
            }
        },
        "parent_map": {
            "model.mri.stg_sales": ["source.mri.silver.sales"],
            "model.mri.fact_ventes": ["model.mri.stg_sales"],
            "test.mri.assert_sales": ["model.mri.fact_ventes"],
        },
    }
    path = _write(tmp_path / "manifest.json", manifest)
    graph = LineageGraph()
    assert add_dbt_graph(graph, path) == 2
    assert graph.has_edge(
        "postgres:silver.sales", "postgres:dbt_staging.stg_sales", "dbt.stg_sales"
    )
    assert graph.has_edge(
        "postgres:dbt_staging.stg_sales", "postgres:dbt_marts.fact_ventes", "dbt.fact_ventes"
    )
    assert all(e.kind != KIND_OBSERVED for e in graph.edge_list())


def test_superset_seed_parsing_maps_tables_and_dashboards(tmp_path: Path) -> None:
    seed = (
        'SALES_SQL = """\nSELECT * FROM warehouse.fact_ventes v\n'
        'JOIN warehouse.dim_date d ON d.date_sk = v.date_sk\n""".strip()\n'
        'AUDIT_SQL = """\nSELECT * FROM audit.load_audit\n""".strip()\n'
        'DATASETS = [("sales_analytics", SALES_SQL), ("platform_health", AUDIT_SQL)]\n'
        'DASHBOARDS = [{"title": "Sales Performance", "dataset": "sales_analytics"}]\n'
    )
    path = tmp_path / "seed.py"
    path.write_text(seed, encoding="utf-8")
    graph = LineageGraph()
    add_superset_graph(graph, path)
    assert graph.has_edge(
        "postgres:warehouse.fact_ventes", "superset:dataset.sales_analytics", "superset.dataset"
    )
    assert graph.has_edge(
        "postgres:audit.load_audit", "superset:dataset.platform_health", "superset.dataset"
    )
    assert graph.has_edge(
        "superset:dataset.sales_analytics",
        "superset:dashboard.Sales Performance",
        "superset.dashboard",
    )
    # exactly the two referenced tables, no Cartesian expansion
    produced = [e for e in graph.edge_list() if e.job == "superset.dataset"]
    assert len(produced) == 3


# ------------------------------------------------------------------------ observed


def _observed_graph(tmp_path: Path) -> LineageGraph:
    events_dir = tmp_path / "events"
    ing = tmp_path / "ingestion"
    brz = tmp_path / "bronze"
    _write(
        ing / "run-ing.json",
        {
            "run_id": "run-ing",
            "records": [
                {
                    "outcome": "ingested",
                    "source": "sales",
                    "original_filename": "sales/a.csv",
                    "object_key": "raw/sales/a.csv",
                    "sha256": "deadbeef",
                }
            ],
        },
    )
    _write(
        brz / "run-brz.json",
        {
            "run_id": "run-brz",
            "records": [
                {
                    "outcome": "bronzed",
                    "source": "sales",
                    "batch": "a",
                    "raw_key": "raw/sales/a.csv",
                    "part_keys": ["bronze/sales/part-0.parquet"],
                    "promoted_key": "_promoted/sales/a.json",
                }
            ],
        },
    )
    _emit(events_dir, run_id="run-ing", pipeline="ingestion", stage="ingest", source="sales")
    _emit(
        events_dir,
        run_id="run-ext",
        pipeline="ingestion",
        stage="external",
        source="frankfurter_fx",
        payload={"object_key": "raw_external/fx/response.json"},
    )
    _emit(events_dir, run_id="run-brz", pipeline="bronze", stage="bronze", source="sales")
    _emit(
        events_dir,
        run_id="run-silv",
        pipeline="silver",
        stage="silver",
        source="sales",
        payload={"target_table": "sales", "marker_key": "_promoted/sales/a.json"},
    )
    _emit(
        events_dir,
        run_id="run-wh",
        pipeline="warehouse",
        stage="warehouse",
        source="sales",
        batch_id="fact_ventes",
        records_written=5,
    )
    _emit(
        events_dir,
        run_id="run-dbt",
        pipeline="dbt",
        stage="dbt",
        source="mri",
        batch_id="stg_sales",
        payload={"node": "model.mri.stg_sales"},
    )
    graph = LineageGraph(dag_run_id=DAG)
    add_observed(
        graph,
        events_dir=events_dir,
        ingestion_manifest_dir=ing,
        bronze_manifest_dir=brz,
        bucket_raw="rawbucket",
        bucket_bronze="bronzebucket",
        silver_schema="silver",
        warehouse_schema="warehouse",
        dag_run_id=DAG,
    )
    return graph


def test_observed_edges_are_exact_and_nothing_is_invented(tmp_path: Path) -> None:
    graph = _observed_graph(tmp_path)
    assert graph.has_edge("file:sales/a.csv", "minio:rawbucket/raw/sales/a.csv", "ingest")
    assert graph.has_edge(
        "api:frankfurter_fx", "minio:rawbucket/raw_external/fx/response.json", "external"
    )
    assert graph.has_edge(
        "minio:rawbucket/raw/sales/a.csv",
        "minio:bronzebucket/bronze/sales/part-0.parquet",
        "bronze",
    )
    assert graph.has_edge(
        "minio:bronzebucket/_promoted/sales/a.json",
        "postgres:silver.sales",
        "silver.load",
    )
    assert graph.has_edge(
        "postgres:silver.sales", "postgres:warehouse.fact_ventes", "warehouse.load"
    )
    # the file is never wired straight to a warehouse table
    assert not any(
        e.upstream == "file:sales/a.csv" and e.downstream.startswith("postgres:")
        for e in graph.edge_list()
    )
    assert graph.edges[
        ("file:sales/a.csv", "minio:rawbucket/raw/sales/a.csv", "ingest")
    ].run_id == ("run-ing")


def test_failed_run_does_not_produce_observed_edges(tmp_path: Path) -> None:
    events_dir = tmp_path / "events"
    _emit(
        events_dir,
        run_id="run-silv",
        pipeline="silver",
        stage="silver",
        status="failed",
        source="sales",
        payload={"target_table": "sales", "marker_key": "_promoted/sales/a.json"},
    )
    graph = LineageGraph(dag_run_id=DAG)
    add_observed(
        graph,
        events_dir=events_dir,
        ingestion_manifest_dir=tmp_path / "ingestion",
        bronze_manifest_dir=tmp_path / "bronze",
        bucket_raw="rawbucket",
        bucket_bronze="bronzebucket",
        silver_schema="silver",
        warehouse_schema="warehouse",
        dag_run_id=DAG,
    )
    assert not any(e.job == "silver.load" for e in graph.edge_list())


def test_missing_manifest_yields_no_file_edge(tmp_path: Path) -> None:
    events_dir = tmp_path / "events"
    _emit(events_dir, run_id="run-ing", pipeline="ingestion", stage="ingest", source="sales")
    graph = LineageGraph(dag_run_id=DAG)
    add_observed(
        graph,
        events_dir=events_dir,
        ingestion_manifest_dir=tmp_path / "missing",
        bronze_manifest_dir=tmp_path / "missing",
        bucket_raw="rawbucket",
        bucket_bronze="bronzebucket",
        silver_schema="silver",
        warehouse_schema="warehouse",
        dag_run_id=DAG,
    )
    assert not any(e.job == "ingest" for e in graph.edge_list())


# ------------------------------------------------------------------- classification


def test_promote_observed_upgrades_executed_dbt_edges() -> None:
    executed_edge = Edge(
        "postgres:silver.sales",
        "postgres:dbt_staging.stg_sales",
        "dbt.stg_sales",
        KIND_DECLARED,
        node="model.mri.stg_sales",
    )
    quiet_edge = Edge(
        "postgres:silver.fx_rates",
        "postgres:dbt_staging.stg_fx",
        "dbt.stg_fx",
        KIND_DECLARED,
        node="model.mri.stg_fx",
    )
    graph = LineageGraph()
    graph.add_edge(executed_edge)
    graph.add_edge(quiet_edge)
    promoted = promote_observed(graph, {"model.mri.stg_sales"})
    assert promoted == 1
    assert graph.edges[executed_edge.key].kind == KIND_INFERRED
    assert graph.edges[quiet_edge.key].kind == KIND_DECLARED


def test_mark_unavailable_flags_referenced_but_unproduced_datasets() -> None:
    graph = LineageGraph()
    graph.add_dataset(Dataset("file", "x", "source"))
    graph.add_dataset(Dataset("minio", "raw/x", "raw"))
    graph.add_dataset(Dataset("postgres", "silver.x", LAYER_SILVER))
    graph.add_dataset(Dataset("postgres", "warehouse.y", LAYER_WAREHOUSE))
    graph.add_dataset(Dataset("postgres", "audit.load_audit", LAYER_WAREHOUSE))
    graph.add_edge(Edge("file:x", "minio:raw/x", "ingest", KIND_OBSERVED))
    graph.add_edge(Edge("minio:raw/x", "postgres:silver.x", "silver.load", KIND_OBSERVED))
    graph.add_edge(
        Edge("postgres:silver.x", "postgres:warehouse.y", "warehouse.load", KIND_OBSERVED)
    )
    graph.add_edge(
        Edge("postgres:audit.load_audit", "postgres:warehouse.y", "superset.dataset", KIND_DECLARED)
    )
    marked = mark_unavailable(graph)
    unavailable = [e for e in graph.edge_list() if e.kind == KIND_UNAVAILABLE]
    assert marked == 1
    assert unavailable[0].downstream == "postgres:audit.load_audit"


# -------------------------------------------------------------------------- end to end


def _write_synthetic(tmp_path: Path) -> dict[str, Path]:
    events_dir = tmp_path / "events"
    ing = tmp_path / "ingestion"
    brz = tmp_path / "bronze"
    _write(
        ing / "run-ing.json",
        {
            "run_id": "run-ing",
            "records": [
                {
                    "outcome": "ingested",
                    "source": "sales",
                    "original_filename": "sales/a.csv",
                    "object_key": "raw/sales/a.csv",
                    "sha256": "deadbeef",
                }
            ],
        },
    )
    _write(
        brz / "run-brz.json",
        {
            "run_id": "run-brz",
            "records": [
                {
                    "outcome": "bronzed",
                    "source": "sales",
                    "batch": "a",
                    "raw_key": "raw/sales/a.csv",
                    "part_keys": ["bronze/sales/part-0.parquet"],
                    "promoted_key": "_promoted/sales/a.json",
                }
            ],
        },
    )
    _emit(events_dir, run_id="run-ing", pipeline="ingestion", stage="ingest", source="sales")
    _emit(events_dir, run_id="run-brz", pipeline="bronze", stage="bronze", source="sales")
    _emit(
        events_dir,
        run_id="run-silv",
        pipeline="silver",
        stage="silver",
        source="sales",
        payload={"target_table": "sales", "marker_key": "_promoted/sales/a.json"},
    )
    _emit(
        events_dir,
        run_id="run-dbt",
        pipeline="dbt",
        stage="dbt",
        payload={"node": "model.mri.stg_sales"},
    )
    manifest = _write(
        tmp_path / "manifest.json",
        {
            "nodes": {
                "model.mri.stg_sales": {
                    "resource_type": "model",
                    "name": "stg_sales",
                    "config": {"schema": "dbt_staging"},
                }
            },
            "sources": {
                "source.mri.silver.sales": {
                    "resource_type": "source",
                    "name": "sales",
                    "source_name": "silver",
                }
            },
            "parent_map": {"model.mri.stg_sales": ["source.mri.silver.sales"]},
        },
    )
    run_results = _write(
        tmp_path / "run_results.json",
        {"results": [{"unique_id": "model.mri.stg_sales", "status": "success"}]},
    )
    seed = tmp_path / "seed.py"
    seed.write_text(
        'SALES_SQL = """\nSELECT * FROM warehouse.fact_ventes\n""".strip()\n'
        'AUDIT_SQL = """\nSELECT * FROM audit.load_audit\n""".strip()\n'
        'DATASETS = [("sales_analytics", SALES_SQL), ("platform_health", AUDIT_SQL)]\n'
        'DASHBOARDS = [{"title": "Sales Performance", "dataset": "sales_analytics"}]\n',
        encoding="utf-8",
    )
    settings = Settings.from_env(
        observability_dir=events_dir,
        manifest_dir=ing,
        bronze_manifest_dir=brz,
    )
    return {
        "events": settings.observability_dir,
        "ingestion": settings.manifest_dir,
        "bronze": settings.bronze_manifest_dir,
        "manifest": manifest,
        "run_results": run_results,
        "seed": seed,
    }


def _full_build(tmp_path: Path) -> LineageGraph:
    paths = _write_synthetic(tmp_path)
    settings = Settings.from_env(
        observability_dir=paths["events"],
        manifest_dir=paths["ingestion"],
        bronze_manifest_dir=paths["bronze"],
    )
    return build_lineage(
        settings,
        dag_run_id=DAG,
        dbt_manifest=paths["manifest"],
        dbt_run_results=paths["run_results"],
        superset_seed=paths["seed"],
        events_dir=paths["events"],
        ingestion_manifest_dir=paths["ingestion"],
        bronze_manifest_dir=paths["bronze"],
    )


def test_full_build_classifies_all_four_kinds(tmp_path: Path) -> None:
    graph = _full_build(tmp_path)
    counts = graph.kind_counts()
    assert counts[KIND_OBSERVED] >= 4
    assert counts[KIND_INFERRED] == 1
    assert counts[KIND_DECLARED] >= 1
    assert counts[KIND_UNAVAILABLE] >= 1
    assert graph.summary()["dag_run_id"] == DAG
    assert graph.runs and graph.runs[0]["run_id"]


def test_openlineage_events_are_well_formed_and_stable(tmp_path: Path) -> None:
    graph = _full_build(tmp_path)
    events = iter_run_events(graph)
    assert events
    jobs = {event["job"]["name"] for event in events}
    assert "ingest" in jobs
    assert "dbt.stg_sales" in jobs
    for event in events:
        assert event["eventType"] == "COMPLETE"
        assert event["_schemaURL"] == SCHEMA_URL
        assert event["run"]["runId"]
        assert event["outputs"]
    assert [e["run"]["runId"] for e in events] == [
        e["run"]["runId"] for e in iter_run_events(graph)
    ]


def test_mermaid_renders_layers_and_edges(tmp_path: Path) -> None:
    graph = _full_build(tmp_path)
    document = render_mermaid(graph)
    assert document.startswith("flowchart LR")
    assert "-->|ingest|" in document
    assert "subgraph silver" in document


def test_cli_build_and_trace(tmp_path: Path) -> None:
    paths = _write_synthetic(tmp_path)
    graph = _full_build(tmp_path)
    out = tmp_path / "lineage.json"
    ol = tmp_path / "openlineage.jsonl"
    mmd = tmp_path / "lineage.mmd"
    overrides = [
        "--events-dir",
        str(paths["events"]),
        "--ingestion-manifests",
        str(paths["ingestion"]),
        "--bronze-manifests",
        str(paths["bronze"]),
        "--dbt-manifest",
        str(paths["manifest"]),
        "--dbt-run-results",
        str(paths["run_results"]),
        "--superset-seed",
        str(paths["seed"]),
    ]
    code = main(
        [
            "build",
            "--dag-run-id",
            DAG,
            "--out",
            str(out),
            "--openlineage-out",
            str(ol),
            "--mermaid-out",
            str(mmd),
            *overrides,
        ]
    )
    assert code == EXIT_OK
    payload = json.loads(out.read_text(encoding="utf-8"))
    assert payload["dag_run_id"] == DAG
    assert payload["summary"]["edges"] == len(graph.edges)
    assert ol.read_text(encoding="utf-8").count("\n") >= 1
    assert mmd.read_text(encoding="utf-8").startswith("flowchart LR")

    assert (
        main(["trace", "--dataset", "postgres:silver.sales", "--dag-run-id", DAG, *overrides])
        == EXIT_OK
    )
    assert (
        main(["trace", "--dataset", "postgres:nope", "--dag-run-id", DAG, *overrides])
        == EXIT_FAILURE
    )
