"""Phase 19: unit tests for the metric sources (SQL + pure row mappers)."""

from __future__ import annotations

from typing import Any

from mri.metrics import sources
from mri.metrics.model import MetricFamily

Row = dict[str, Any]


def by_name(families: list[MetricFamily]) -> dict[str, MetricFamily]:
    return {family.name: family for family in families}


def values(family: MetricFamily) -> dict[tuple[tuple[str, str], ...], float]:
    return {sample.labels: sample.value for sample in family.samples}


def test_pipeline_metric_families_emit_all_nine_metrics() -> None:
    rows: list[Row] = [
        {
            "pipeline": "ingestion",
            "stage": "validate",
            "runs": 3,
            "succeeded": 2,
            "failed": 1,
            "success_rate": 66.67,
            "rows_read": 30,
            "rows_written": 29,
            "rows_rejected": 1,
            "duration_ms_avg": 12.5,
            "duration_ms_p95": 20.0,
        }
    ]
    families = by_name(sources.pipeline_metric_families(rows))
    assert set(families) == {
        "mri_pipeline_runs",
        "mri_pipeline_succeeded",
        "mri_pipeline_failed",
        "mri_pipeline_success_rate",
        "mri_pipeline_rows_read",
        "mri_pipeline_rows_written",
        "mri_pipeline_rows_rejected",
        "mri_pipeline_duration_ms_avg",
        "mri_pipeline_duration_ms_p95",
    }
    runs = families["mri_pipeline_runs"]
    assert list(values(runs).values()) == [3.0]
    assert runs.samples[0].labels == (("pipeline", "ingestion"), ("stage", "validate"))


def test_pipeline_metric_families_coerce_null_counts_to_zero() -> None:
    row: Row = {"pipeline": "bronze", "stage": "bronze"}
    for column in (
        "runs",
        "succeeded",
        "failed",
        "success_rate",
        "rows_read",
        "rows_written",
        "rows_rejected",
        "duration_ms_avg",
        "duration_ms_p95",
    ):
        row[column] = None
    families = by_name(sources.pipeline_metric_families([row]))
    assert all(sample.value == 0.0 for family in families.values() for sample in family.samples)


def test_latest_run_families_skip_missing_timestamps_and_encode_outcome() -> None:
    rows: list[Row] = [
        {
            "pipeline": "silver",
            "stage": "silver",
            "last_run_at": 1000,
            "last_success_at": None,
            "last_run_succeeded": False,
        },
        {
            "pipeline": "bronze",
            "stage": "bronze",
            "last_run_at": 2000,
            "last_success_at": 2000,
            "last_run_succeeded": True,
        },
    ]
    families = by_name(sources.latest_run_families(rows))
    runs = values(families["mri_pipeline_last_run_timestamp_seconds"])
    assert len(runs) == 2
    success = values(families["mri_pipeline_last_success_timestamp_seconds"])
    assert len(success) == 1  # silver has no successful run
    outcomes = values(families["mri_pipeline_last_run_succeeded"])
    assert sorted(outcomes.values()) == [0.0, 1.0]


def test_freshness_families_skip_missing_last_success() -> None:
    rows: list[Row] = [
        {"source": "sales", "staleness_seconds": 60, "last_success_at": 1000},
        {"source": "fx", "staleness_seconds": None, "last_success_at": None},
    ]
    families = by_name(sources.freshness_families(rows))
    assert len(families["mri_source_freshness_seconds"].samples) == 2
    assert len(families["mri_source_last_success_timestamp_seconds"].samples) == 1


def test_warehouse_reconcile_families_encode_balance_and_both_sides() -> None:
    rows: list[Row] = [
        {
            "target_table": "fact_ventes",
            "source_table": "silver_fact_ventes",
            "status": "success",
            "records_read": 2000,
            "records_written": 2000,
        },
        {
            "target_table": "fact_stock",
            "source_table": "silver_fact_stock",
            "status": "failed",
            "records_read": 10,
            "records_written": 8,
        },
    ]
    families = by_name(sources.warehouse_reconcile_families(rows))
    balanced = values(families["mri_warehouse_reconcile_balanced"])
    assert balanced == {(("table", "fact_stock"),): 0.0, (("table", "fact_ventes"),): 1.0}
    rows_metric = families["mri_warehouse_reconcile_rows"]
    assert len(rows_metric.samples) == 4  # two sides per table
    sides = {dict(sample.labels)["side"] for sample in rows_metric.samples}
    assert sides == {"silver", "warehouse"}


def test_postgres_families_expose_size_cache_deadlocks_and_connections() -> None:
    db_rows: list[Row] = [
        {
            "datname": "retail",
            "size_bytes": 123,
            "connections": 7,
            "cache_hit_ratio": 0.99,
            "deadlocks": 0,
        }
    ]
    activity_rows: list[Row] = [
        {"state": "active", "connections": 1},
        {"state": "idle", "connections": 4},
    ]
    families = by_name(sources.postgres_families(db_rows, activity_rows))
    assert set(families) == {
        "mri_postgres_database_size_bytes",
        "mri_postgres_cache_hit_ratio",
        "mri_postgres_deadlocks",
        "mri_postgres_connections",
    }
    assert len(families["mri_postgres_connections"].samples) == 2


def test_airflow_families_expose_runs_tasks_and_last_run() -> None:
    dag_rows: list[Row] = [
        {"dag_id": "morocco_retail_pipeline", "state": "success", "dag_runs": 5},
        {"dag_id": "morocco_retail_pipeline", "state": "failed", "dag_runs": 1},
    ]
    last_rows: list[Row] = [{"dag_id": "morocco_retail_pipeline", "last_run_at": 999}]
    task_rows: list[Row] = [
        {"dag_id": "morocco_retail_pipeline", "state": "success", "task_instances": 42}
    ]
    families = by_name(sources.airflow_families(dag_rows, last_rows, task_rows))
    assert len(families["mri_airflow_dag_runs"].samples) == 2
    assert len(families["mri_airflow_task_instances"].samples) == 1
    assert values(families["mri_airflow_last_dag_run_timestamp_seconds"]) == {
        (("dag_id", "morocco_retail_pipeline"),): 999.0
    }


def test_exporter_self_families_omit_airflow_when_not_configured() -> None:
    families = by_name(
        sources.exporter_self_families(
            version="0.1.0",
            last_scrape_success=1.0,
            scrape_duration_seconds=0.5,
            postgres_up=1.0,
            airflow_up=None,
            retail_errors_total=0.0,
            airflow_errors_total=None,
        )
    )
    assert "mri_airflow_up" not in families
    errors = families["mri_exporter_scrape_errors_total"]
    assert [sample.labels for sample in errors.samples] == [(("source", "retail"),)]
    assert errors.type == "counter"
    assert families["mri_exporter_build_info"].samples[0].labels == (("version", "0.1.0"),)


def test_exporter_self_families_include_airflow_when_configured() -> None:
    families = by_name(
        sources.exporter_self_families(
            version="0.1.0",
            last_scrape_success=0.0,
            scrape_duration_seconds=0.5,
            postgres_up=1.0,
            airflow_up=0.0,
            retail_errors_total=0.0,
            airflow_errors_total=3.0,
        )
    )
    assert "mri_airflow_up" in families
    assert len(families["mri_exporter_scrape_errors_total"].samples) == 2


def test_sql_targets_existing_ops_views_and_airflow_metadata() -> None:
    joined = "\n".join(
        [
            sources.PIPELINE_METRICS_SQL,
            sources.LATEST_RUN_SQL,
            sources.FRESHNESS_SQL,
            sources.WAREHOUSE_RECONCILE_SQL,
        ]
    )
    assert "ops.v_pipeline_metrics" in sources.PIPELINE_METRICS_SQL
    assert "ops.v_run_facts" in sources.LATEST_RUN_SQL
    assert "ops.v_freshness" in sources.FRESHNESS_SQL
    assert "ops.pipeline_event" in sources.WAREHOUSE_RECONCILE_SQL
    assert "dag_run" in sources.AIRFLOW_DAG_RUN_SQL
    assert "task_instance" in sources.AIRFLOW_TASK_SQL
    # bounded labels: no per-run/batch/file identifiers are selected out as labels
    assert "run_id AS" not in joined
