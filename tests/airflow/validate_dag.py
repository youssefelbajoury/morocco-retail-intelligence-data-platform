"""Parse ``morocco_retail_pipeline`` with Airflow's DagBag inside a container.

The host has no Airflow installation by design (PHASE 11 constraint), so the
DAG is validated with the *same* Airflow Python API that the running
scheduler/dag-processor uses. Host-side assertions live in
``tests/integration/test_airflow_dag.py``, which executes this script with:

    docker compose --env-file .env \\
      -f infrastructure/docker/docker-compose.yml \\
      -f infrastructure/docker/docker-compose.airflow.yml \\
      exec -T airflow-scheduler python /opt/airflow/mri/tests/airflow/validate_dag.py --json

Exit code 0 means every structural check passed.
"""

from __future__ import annotations

import argparse
import importlib
import json
import os
import sys
from pathlib import Path
from typing import Any

os.environ.setdefault("AIRFLOW__LOGGING__LOGGING_LEVEL", "WARNING")

DagBag = importlib.import_module("airflow.models").DagBag

DAG_ID = "morocco_retail_pipeline"
DEFAULT_DAGS_FOLDER = "/opt/airflow/dags"

EXPECTED_TASK_IDS = [
    "audit_airflow_run",
    "bronze_internal_external",
    "dbt_build",
    "ingest_external",
    "ingest_internal",
    "reconcile",
    "silver_load",
    "warehouse_build_reference",
]
EXPECTED_EDGES = [
    ["bronze_internal_external", "silver_load"],
    ["dbt_build", "reconcile"],
    ["ingest_external", "bronze_internal_external"],
    ["ingest_internal", "bronze_internal_external"],
    ["reconcile", "audit_airflow_run"],
    ["silver_load", "warehouse_build_reference"],
    ["warehouse_build_reference", "dbt_build"],
]
EXPECTED_TASK_COUNT = 8
EXPECTED_RETRIES = 2


def _edges(dag: Any) -> list[list[str]]:
    return sorted(
        [task.task_id, downstream] for task in dag.tasks for downstream in task.downstream_task_ids
    )


def _has_cycle(dag: Any) -> bool:
    indegree = {task.task_id: len(task.upstream_task_ids) for task in dag.tasks}
    ready = [task_id for task_id, count in indegree.items() if count == 0]
    visited = 0
    while ready:
        node = ready.pop()
        visited += 1
        for downstream in dag.task_dict[node].downstream_task_ids:
            indegree[downstream] -= 1
            if indegree[downstream] == 0:
                ready.append(downstream)
    return visited != len(indegree)


def validate(dags_folder: Path) -> dict[str, Any]:
    bag = DagBag(dag_folder=str(dags_folder))
    import_errors = {
        path: str(exc).splitlines()[0] for path, exc in sorted(bag.import_errors.items())
    }
    dag = bag.dags.get(DAG_ID)
    checks: dict[str, bool] = {"no_import_errors": not import_errors, "dag_found": dag is not None}
    task_ids: list[str] = []
    edges: list[list[str]] = []
    schedule = None
    catchup = None
    max_active_runs = None

    if dag is not None:
        task_ids = sorted(task.task_id for task in dag.tasks)
        edges = _edges(dag)
        expression = getattr(getattr(dag, "timetable", None), "expression", None)
        schedule = str(expression) if expression is not None else None
        catchup = bool(dag.catchup)
        max_active_runs = dag.max_active_runs
        checks["task_count"] = len(dag.tasks) == EXPECTED_TASK_COUNT
        checks["task_ids"] = task_ids == EXPECTED_TASK_IDS
        checks["dependencies"] = edges == EXPECTED_EDGES
        checks["no_cycle"] = not _has_cycle(dag)
        checks["all_bash_tasks"] = all(
            task.__class__.__name__ == "BashOperator" for task in dag.tasks
        )
        checks["dag_run_id_correlated"] = all(
            "--dag-run-id" in str(getattr(task, "bash_command", "") or "") for task in dag.tasks
        )
        checks["retries"] = all(task.retries == EXPECTED_RETRIES for task in dag.tasks)
        checks["execution_timeouts"] = all(task.execution_timeout is not None for task in dag.tasks)
        checks["catchup_disabled"] = catchup is False
        checks["single_active_run"] = max_active_runs == 1
        checks["dagrun_timeout"] = dag.dagrun_timeout is not None
        checks["schedule_configured"] = bool(schedule)

    return {
        "dag_id": DAG_ID,
        "dags_folder": str(dags_folder),
        "import_errors": import_errors,
        "task_count": len(task_ids),
        "task_ids": task_ids,
        "edges": edges,
        "schedule": schedule,
        "catchup": catchup,
        "max_active_runs": max_active_runs,
        "checks": checks,
        "failed_checks": sorted(name for name, passed in checks.items() if not passed),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dags-folder",
        default=os.environ.get("MRI_DAGS_FOLDER", DEFAULT_DAGS_FOLDER),
        type=Path,
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args(argv)

    report = validate(args.dags_folder)

    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"dag_id:        {report['dag_id']}")
        print(f"task_count:    {report['task_count']}")
        print(f"task_ids:      {', '.join(report['task_ids'])}")
        print(f"edges:         {len(report['edges'])}")
        print(f"schedule:      {report['schedule']}")
        print(f"import_errors: {len(report['import_errors'])}")
        for name in report["failed_checks"]:
            print(f"FAILED check:  {name}")

    return 0 if not report["failed_checks"] else 1


if __name__ == "__main__":
    sys.exit(main())
