"""Command-line interface for platform data lineage.

Usage examples::

    python -m apps.lineage build
    python -m apps.lineage build --dag-run-id dag__e2e_va7n3q8r
    python -m apps.lineage build --dag-run-id latest --skip-mermaid
    python -m apps.lineage trace --dataset postgres:silver.sales --direction all
    python -m apps.lineage dags

``build`` writes ``reports/lineage/lineage.json``, ``openlineage.jsonl`` and
``lineage.mmd``. ``trace`` answers "where does this dataset come from / feed
into?". All commands read ``Settings.from_env()`` so every ``PG_*``/``MINIO_*``
variable in the repo ``.env`` is honoured.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from mri.ingestion.config import Settings
from mri.ingestion.exceptions import UsageError
from mri.lineage.build import (
    DEFAULT_MANIFEST,
    DEFAULT_RUN_RESULTS,
    DEFAULT_SEED,
    build_lineage,
)
from mri.lineage.mermaid import write_mermaid
from mri.lineage.model import LineageGraph
from mri.lineage.openlineage import emit_openlineage, iter_run_events
from mri.observability.store import EventLog

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_USAGE = 2

DEFAULT_OUT_DIR = Path("reports/lineage")


def list_dag_runs(events_dir: Path) -> list[str]:
    """DAG run ids in append (chronological) order, de-duplicated."""
    seen: list[str] = []
    for event in EventLog.resolve(events_dir).read_events():
        dag = event.dag_run_id
        if dag and dag not in seen:
            seen.append(dag)
    return seen


def _resolve_dag_run_id(args: argparse.Namespace, settings: Settings) -> str:
    if args.dag_run_id not in (None, "", "latest"):
        return str(args.dag_run_id)
    dags = list_dag_runs(args.events_dir or settings.observability_dir)
    if not dags:
        raise UsageError("no DAG runs found in the event log")
    return dags[-1]


def _graph_for(args: argparse.Namespace, settings: Settings, dag_run_id: str) -> LineageGraph:
    return build_lineage(
        settings,
        dag_run_id=dag_run_id,
        dbt_manifest=args.dbt_manifest,
        dbt_run_results=args.dbt_run_results,
        superset_seed=args.superset_seed,
        events_dir=args.events_dir or settings.observability_dir,
        ingestion_manifest_dir=args.ingestion_manifests or settings.manifest_dir,
        bronze_manifest_dir=args.bronze_manifests or settings.bronze_manifest_dir,
    )


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _cmd_build(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    dag_run_id = _resolve_dag_run_id(args, settings)
    graph = _graph_for(args, settings, dag_run_id)
    lineage_out = args.out or (DEFAULT_OUT_DIR / "lineage.json")
    _write_json(lineage_out, graph.to_dict())
    messages = [f"wrote {lineage_out}"]
    if not args.skip_openlineage:
        events = iter_run_events(graph)
        ol_path = args.openlineage_out or (DEFAULT_OUT_DIR / "openlineage.jsonl")
        count = emit_openlineage(ol_path, events)
        messages.append(f"wrote {ol_path} ({count} events)")
    if not args.skip_mermaid:
        mmd_path = args.mermaid_out or (DEFAULT_OUT_DIR / "lineage.mmd")
        count = write_mermaid(mmd_path, graph)
        messages.append(f"wrote {mmd_path} ({count} edges)")
    if args.as_json:
        print(json.dumps(graph.to_dict(), indent=2, ensure_ascii=False))
    else:
        summary = graph.summary()
        print(f"dag_run_id={summary['dag_run_id'] or '(declared only)'}")
        print(f"datasets={summary['datasets']} edges={summary['edges']} runs={summary['runs']}")
        print("edges_by_kind=" + json.dumps(summary["edges_by_kind"]))
        print("datasets_by_layer=" + json.dumps(summary["datasets_by_layer"]))
        for message in messages:
            print(message)
    return EXIT_OK


def _trace_print(graph: LineageGraph, dataset_id: str, direction: str) -> None:
    edges = graph.edge_list()
    if direction in ("upstream", "all"):
        frontier = [dataset_id]
        seen: set[str] = set()
        chain: list[str] = []
        while frontier:
            current = frontier.pop(0)
            if current in seen:
                continue
            seen.add(current)
            parents = [e.upstream for e in edges if e.downstream == current]
            chain.append(current)
            frontier.extend(parents)
        for item in reversed(chain):
            print(f"  {item}")
    if direction in ("downstream", "all"):
        frontier = [dataset_id]
        seen = set()
        while frontier:
            current = frontier.pop(0)
            if current in seen:
                continue
            seen.add(current)
            print(f"  {current}")
            frontier.extend(e.downstream for e in edges if e.upstream == current)


def _cmd_trace(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    dag_run_id = _resolve_dag_run_id(args, settings)
    graph = _graph_for(args, settings, dag_run_id)
    dataset_id = args.dataset
    if dataset_id not in graph.datasets:
        print(f"unknown dataset: {dataset_id}", file=sys.stderr)
        print(f"known dataset ids are in {DEFAULT_OUT_DIR / 'lineage.json'}", file=sys.stderr)
        return EXIT_FAILURE
    print(f"lineage for {dataset_id} (dag_run_id={graph.dag_run_id or '(declared)'})")
    _trace_print(graph, dataset_id, args.direction)
    return EXIT_OK


def _cmd_dags(args: argparse.Namespace) -> int:
    settings = Settings.from_env()
    dags = list_dag_runs(args.events_dir or settings.observability_dir)
    for dag in dags:
        print(dag)
    if not dags:
        print("no DAG runs recorded yet", file=sys.stderr)
        return EXIT_FAILURE
    return EXIT_OK


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="apps.lineage")
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="build and write the lineage artifacts")
    trace = sub.add_parser("trace", help="trace one dataset through the graph")
    dags = sub.add_parser("dags", help="list DAG run ids recorded in the event log")

    for command in (build, trace):
        command.add_argument(
            "--dag-run-id", default="latest", help="run to observe; default: latest DAG run"
        )
        command.add_argument("--events-dir", type=Path)
        command.add_argument("--ingestion-manifests", type=Path)
        command.add_argument("--bronze-manifests", type=Path)
        command.add_argument("--dbt-manifest", type=Path, default=None)
        command.add_argument("--dbt-run-results", type=Path, default=None)
        command.add_argument("--superset-seed", type=Path, default=None)

    build.add_argument("--out", type=Path)
    build.add_argument("--openlineage-out", type=Path)
    build.add_argument("--mermaid-out", type=Path)
    build.add_argument("--skip-openlineage", action="store_true")
    build.add_argument("--skip-mermaid", action="store_true")
    build.add_argument("--json", dest="as_json", action="store_true")

    trace.add_argument("--dataset", required=True, help="dataset id, e.g. postgres:silver.sales")
    trace.add_argument("--direction", choices=("upstream", "downstream", "all"), default="all")

    dags.add_argument("--events-dir", type=Path)
    return parser


def _apply_path_defaults(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    if getattr(args, "dbt_manifest", None) is None:
        args.dbt_manifest = DEFAULT_MANIFEST
    if getattr(args, "dbt_run_results", None) is None:
        args.dbt_run_results = DEFAULT_RUN_RESULTS
    if getattr(args, "superset_seed", None) is None:
        args.superset_seed = DEFAULT_SEED


def main(argv: Sequence[str] | None = None) -> int:
    """Run the lineage CLI and return its exit code."""
    parser = _parser()
    args = parser.parse_args(argv)
    _apply_path_defaults(args, parser)
    try:
        if args.command == "build":
            return _cmd_build(args)
        if args.command == "trace":
            return _cmd_trace(args)
        if args.command == "dags":
            return _cmd_dags(args)
    except UsageError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    return EXIT_USAGE


__all__ = ["EXIT_FAILURE", "EXIT_OK", "EXIT_USAGE", "list_dag_runs", "main"]
