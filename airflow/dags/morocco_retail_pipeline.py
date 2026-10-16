"""End-to-end orchestration for the Morocco Retail Intelligence pipeline.

Orchestration only: every task shells out to an existing repository CLI, so
all business logic stays in ``src/mri``, ``apps`` and ``dbt``. The DAG is
executed by the official Apache Airflow 3.3.2 containers (LocalExecutor)
defined in ``infrastructure/docker/docker-compose.airflow.yml``.

Configuration is environmental: schedule, retry policy and repository paths
are read from container environment variables (see
``docs/orchestration/airflow.md``). Idempotence is delegated to the existing
layers (content-addressed raw batches, ``already_fetched`` external marker,
load audit registry, truncate-and-rebuild warehouse, deterministic dbt
models) — nothing here prevents duplicate work.

Every DAG task carries ``--dag-run-id "{{ dag_run.run_id }}"`` so the Phase 16
structured audit log (``reports/observability/events.jsonl``) can correlate
all stages of one orchestrated run; a final ``audit_airflow_run`` task writes
the DAG-level bookend event.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DAG_ID = "morocco_retail_pipeline"

REPO_ROOT = os.environ.get("MRI_REPO_ROOT", "/opt/airflow/mri")
PYTHON = os.environ.get("MRI_PYTHON", "/opt/venv/bin/python")
SCHEDULE = os.environ.get("MRI_DAG_SCHEDULE", "0 4 * * *")
DAG_RUN_ID = '--dag-run-id "{{ dag_run.run_id }}"'
RETRIES = int(os.environ.get("MRI_DAG_RETRIES", "2"))
RETRY_DELAY_MINUTES = int(os.environ.get("MRI_DAG_RETRY_DELAY_MINUTES", "5"))

DEFAULT_ARGS = {
    "owner": "data-platform",
    "start_date": datetime(2026, 1, 1, tzinfo=UTC),
    "retries": RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY_MINUTES),
}


def _bash(
    task_id: str,
    module: str,
    args: str,
    timeout_minutes: int,
    correlate: bool = True,
) -> BashOperator:
    """Wrap ``python -m <module>`` run from the repository root.

    ``PATH`` is prefixed with the project venv's ``bin`` directory so that
    console scripts installed there (``dbt``, ...) are discoverable, exactly
    as ``uv run`` does on the host. When ``correlate`` is set the Airflow DAG
    run id is injected as the first flag so the underlying CLI associates every
    event it emits with the orchestrated run.
    """
    correlation = f" {DAG_RUN_ID}" if correlate else ""
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {REPO_ROOT} && PATH={os.path.dirname(PYTHON)}:$PATH "
            f"{PYTHON} -m {module}{correlation} {args}"
        ),
        execution_timeout=timedelta(minutes=timeout_minutes),
    )


with DAG(
    dag_id=DAG_ID,
    description="Ingest -> Bronze -> Silver -> warehouse -> dbt -> audit",
    default_args=DEFAULT_ARGS,
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["morocco-retail", "batch", "phase-16"],
):
    ingest_internal = _bash(
        "ingest_internal",
        "apps.ingestion",
        "ingest --path data/generated/tiny --recursive",
        30,
    )
    ingest_external = _bash(
        "ingest_external",
        "apps.ingestion",
        "external --source weather --source fx",
        60,
    )
    bronze = _bash("bronze_internal_external", "apps.ingestion", "bronze --source all", 60)
    silver_load = _bash("silver_load", "apps.ingestion", "load --source all", 60)
    warehouse_build = _bash("warehouse_build_reference", "apps.warehouse", "build", 45)
    dbt_build = _bash("dbt_build", "apps.dbt", "build", 60)
    reconcile = _bash(
        "reconcile",
        "apps.dbt",
        "test --select assert_fact_row_counts assert_marts_match_warehouse",
        20,
    )
    audit_airflow_run = _bash(
        "audit_airflow_run",
        "apps.observability",
        'record --dag-run-id "{{ dag_run.run_id }}" '
        "--pipeline airflow --stage dag_run --status success "
        "--logical-date \"{{ data_interval_start | default('') }}\" "
        '--message "DAG run completed"',
        5,
        correlate=False,
    )

    [ingest_internal, ingest_external] >> bronze
    bronze >> silver_load >> warehouse_build >> dbt_build >> reconcile >> audit_airflow_run
