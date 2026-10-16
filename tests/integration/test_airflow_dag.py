"""Phase 11: validate the Airflow DAG with the real Airflow Python API.

The host intentionally has no Airflow installation, so the DAG is parsed with
``airflow.models.DagBag`` inside the running ``airflow-scheduler`` container
(same code path the dag-processor uses). Skipped automatically when Docker or
the Airflow stack is not running; start it with the two-file compose command
documented in ``docs/orchestration/airflow.md``.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE = [
    "docker",
    "compose",
    "--env-file",
    ".env",
    "-f",
    "infrastructure/docker/docker-compose.yml",
    "-f",
    "infrastructure/docker/docker-compose.airflow.yml",
]
VALIDATOR = "/opt/airflow/mri/tests/airflow/validate_dag.py"
DAG_ID = "morocco_retail_pipeline"
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


def _compose(*args: str, timeout: int = 120) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [*COMPOSE, *args],
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=REPO_ROOT,
        check=False,
    )


def _running_scheduler() -> bool:
    result = _compose("ps", "--status", "running", "-q", "airflow-scheduler")
    return bool(result.stdout.strip())


def test_airflow_dag_structure() -> None:
    if shutil.which("docker") is None:
        pytest.skip("docker CLI not available")
    if not _running_scheduler():
        pytest.skip("Airflow stack not running; see docs/orchestration/airflow.md")

    result = _compose("exec", "-T", "airflow-scheduler", "python", VALIDATOR, "--json", timeout=300)
    assert result.returncode == 0, (
        f"in-container DAG validation failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )

    report: dict[str, Any] = json.loads(result.stdout)
    assert report["dag_id"] == DAG_ID
    assert report["import_errors"] == {}
    assert report["failed_checks"] == []
    assert report["task_count"] == 8
    assert report["task_ids"] == EXPECTED_TASK_IDS
    assert report["edges"] == EXPECTED_EDGES
    assert report["catchup"] is False
    assert report["max_active_runs"] == 1
    assert report["schedule"] is not None
