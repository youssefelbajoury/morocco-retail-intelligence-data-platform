"""Phase 19: unit tests for the metrics exporter (no database required)."""

from __future__ import annotations

from typing import Any

from mri.metrics.config import MetricsSettings
from mri.metrics.exporter import MetricsExporter
from mri.metrics.sources import (
    AIRFLOW_DAG_LAST_SQL,
    AIRFLOW_DAG_RUN_SQL,
    AIRFLOW_TASK_SQL,
    FRESHNESS_SQL,
    LATEST_RUN_SQL,
    PIPELINE_METRICS_SQL,
    POSTGRES_ACTIVITY_SQL,
    POSTGRES_DATABASE_SQL,
    WAREHOUSE_RECONCILE_SQL,
)

RETAIL_ROWS: dict[str, list[dict[str, Any]]] = {
    PIPELINE_METRICS_SQL: [
        {
            "pipeline": "bronze",
            "stage": "bronze",
            "runs": 4,
            "succeeded": 4,
            "failed": 0,
            "success_rate": 100.0,
            "rows_read": 40,
            "rows_written": 40,
            "rows_rejected": 0,
            "duration_ms_avg": 10.0,
            "duration_ms_p95": 12.0,
        }
    ],
    LATEST_RUN_SQL: [
        {
            "pipeline": "bronze",
            "stage": "bronze",
            "last_run_at": 1000,
            "last_success_at": 1000,
            "last_run_succeeded": True,
        }
    ],
    FRESHNESS_SQL: [{"source": "sales", "staleness_seconds": 120, "last_success_at": 1000}],
    WAREHOUSE_RECONCILE_SQL: [
        {
            "target_table": "fact_ventes",
            "source_table": "silver_fact_ventes",
            "status": "success",
            "records_read": 10,
            "records_written": 10,
        }
    ],
    POSTGRES_DATABASE_SQL: [
        {
            "datname": "retail",
            "size_bytes": 4096,
            "connections": 3,
            "cache_hit_ratio": 0.9,
            "deadlocks": 0,
        }
    ],
    POSTGRES_ACTIVITY_SQL: [{"state": "active", "connections": 3}],
}

AIRFLOW_ROWS: dict[str, list[dict[str, Any]]] = {
    AIRFLOW_DAG_RUN_SQL: [{"dag_id": "morocco_retail_pipeline", "state": "success", "dag_runs": 2}],
    AIRFLOW_DAG_LAST_SQL: [{"dag_id": "morocco_retail_pipeline", "last_run_at": 2000}],
    AIRFLOW_TASK_SQL: [
        {"dag_id": "morocco_retail_pipeline", "state": "success", "task_instances": 8}
    ],
}


class FakeCursor:
    def __init__(self, store: dict[str, list[dict[str, Any]]]) -> None:
        self._store = store
        self._rows: list[dict[str, Any]] = []

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, sql: str, params: object = None) -> None:
        self._rows = list(self._store.get(sql, []))

    def fetchall(self) -> list[dict[str, Any]]:
        return self._rows


class FakeConnection:
    def __init__(self, store: dict[str, list[dict[str, Any]]]) -> None:
        self.store = store

    def cursor(self, **kwargs: object) -> FakeCursor:
        return FakeCursor(self.store)

    def close(self) -> None:
        return None


def make_connector(
    retail: dict[str, list[dict[str, Any]]] | Exception,
    airflow: dict[str, list[dict[str, Any]]] | Exception | None,
) -> Any:
    retail_dsn = "postgresql://retail"
    airflow_dsn = "postgresql://airflow"

    def connector(dsn: str) -> FakeConnection:
        if dsn == retail_dsn:
            if isinstance(retail, Exception):
                raise retail
            return FakeConnection(retail)
        if dsn == airflow_dsn:
            if isinstance(airflow, Exception):
                raise airflow
            assert airflow is not None
            return FakeConnection(airflow)
        raise AssertionError(f"unexpected dsn: {dsn}")

    return connector


def settings(*, airflow: bool) -> MetricsSettings:
    return MetricsSettings(
        host="127.0.0.1",
        port=9245,
        retail_dsn="postgresql://retail",
        airflow_dsn="postgresql://airflow" if airflow else "",
        version="0.1.0",
    )


def test_collect_emits_domain_and_self_metrics() -> None:
    exporter = MetricsExporter(
        settings(airflow=True),
        connector=make_connector(RETAIL_ROWS, AIRFLOW_ROWS),
        log=lambda _: None,
    )
    text = exporter.collect()
    assert 'mri_pipeline_runs{pipeline="bronze",stage="bronze"} 4' in text
    assert 'mri_source_freshness_seconds{source="sales"} 120' in text
    assert 'mri_warehouse_reconcile_balanced{table="fact_ventes"} 1' in text
    assert 'mri_postgres_database_size_bytes{datname="retail"} 4096' in text
    assert 'mri_airflow_dag_runs{dag_id="morocco_retail_pipeline",state="success"} 2' in text
    assert "mri_exporter_last_scrape_success 1" in text
    assert "mri_airflow_up 1" in text


def test_collect_degrades_safely_when_retail_is_down() -> None:
    exporter = MetricsExporter(
        settings(airflow=False),
        connector=make_connector(RuntimeError("boom"), None),
        log=lambda _: None,
    )
    text = exporter.collect()
    assert "mri_exporter_last_scrape_success 0" in text
    assert "mri_postgres_up 0" in text
    assert 'mri_exporter_scrape_errors_total{source="retail"} 1' in text
    # no stale domain metrics leak through
    assert "mri_pipeline_runs" not in text


def test_collect_marks_failure_when_airflow_scrape_fails() -> None:
    exporter = MetricsExporter(
        settings(airflow=True),
        connector=make_connector(RETAIL_ROWS, RuntimeError("airflow down")),
        log=lambda _: None,
    )
    text = exporter.collect()
    assert "mri_exporter_last_scrape_success 0" in text
    assert "mri_postgres_up 1" in text
    assert "mri_airflow_up 0" in text
    assert 'mri_exporter_scrape_errors_total{source="airflow"} 1' in text
    # retail metrics still present
    assert 'mri_pipeline_runs{pipeline="bronze",stage="bronze"} 4' in text


def test_collect_omits_airflow_series_when_not_configured() -> None:
    exporter = MetricsExporter(
        settings(airflow=False),
        connector=make_connector(RETAIL_ROWS, None),
        log=lambda _: None,
    )
    text = exporter.collect()
    assert "mri_airflow_up" not in text
    assert 'source="airflow"' not in text


def test_error_counter_accumulates_across_scrapes() -> None:
    exporter = MetricsExporter(
        settings(airflow=False),
        connector=make_connector(RuntimeError("boom"), None),
        log=lambda _: None,
    )
    exporter.collect()
    text = exporter.collect()
    assert 'mri_exporter_scrape_errors_total{source="retail"} 2' in text
