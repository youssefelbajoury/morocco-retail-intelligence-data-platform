"""The metrics exporter: read the audit views, emit Prometheus text.

Design points (see ``docs/observability/prometheus-grafana.md``):

* **Existing sources only.** The exporter adds no tracking of its own; it reads
  ``ops.v_pipeline_metrics`` / ``ops.v_run_facts`` / ``ops.v_freshness`` /
  ``ops.pipeline_event`` plus PostgreSQL and Airflow metadata.
* **Bounded labels.** Only low-cardinality dimensions are exposed (pipeline,
  stage, source, table, state, dag_id, datname). Run ids, batch ids and file
  names stay in the audit log.
* **Fail-safe, never misleading.** If a source cannot be queried the exporter
  emits *no* domain metrics for it and sets ``mri_exporter_last_scrape_success``
  to ``0`` (and increments ``mri_exporter_scrape_errors_total``), so a stale or
  partial value can never masquerade as a fresh one.
"""

from __future__ import annotations

import sys
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

from mri.metrics.config import MetricsSettings
from mri.metrics.model import MetricFamily, render_text
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
    airflow_families,
    exporter_self_families,
    freshness_families,
    latest_run_families,
    pipeline_metric_families,
    postgres_families,
    warehouse_reconcile_families,
)

Connector = Callable[[str], Connection[Any]]
Row = dict[str, Any]


def default_connector(dsn: str) -> Connection[Any]:
    """Open a short-lived, autocommit connection suitable for read-only scrapes."""
    return psycopg.connect(dsn, autocommit=True)


class MetricsExporter:
    """Collects metrics on demand and renders the Prometheus text payload."""

    def __init__(
        self,
        settings: MetricsSettings,
        *,
        connector: Connector | None = None,
        log: Callable[[str], None] | None = None,
    ) -> None:
        self.settings = settings
        self._connect: Connector = connector or default_connector
        self._log = log or _stderr
        self._lock = threading.Lock()
        self._retail_errors = 0
        self._airflow_errors = 0

    # -- database helpers -------------------------------------------------

    def _query(self, connection: Connection[Any], sql: str) -> list[Row]:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        return [dict(row) for row in rows]

    def _collect_retail(self) -> list[MetricFamily]:
        families: list[MetricFamily] = []
        connection = self._connect(self.settings.retail_dsn)
        try:
            families += pipeline_metric_families(self._query(connection, PIPELINE_METRICS_SQL))
            families += latest_run_families(self._query(connection, LATEST_RUN_SQL))
            families += freshness_families(self._query(connection, FRESHNESS_SQL))
            families += warehouse_reconcile_families(
                self._query(connection, WAREHOUSE_RECONCILE_SQL)
            )
            database_rows = self._query(connection, POSTGRES_DATABASE_SQL)
            activity_rows = self._query(connection, POSTGRES_ACTIVITY_SQL)
            families += postgres_families(database_rows, activity_rows)
        finally:
            connection.close()
        return families

    def _collect_airflow(self) -> list[MetricFamily]:
        connection = self._connect(self.settings.airflow_dsn)
        try:
            dag_rows = self._query(connection, AIRFLOW_DAG_RUN_SQL)
            last_rows = self._query(connection, AIRFLOW_DAG_LAST_SQL)
            task_rows = self._query(connection, AIRFLOW_TASK_SQL)
        finally:
            connection.close()
        return airflow_families(dag_rows, last_rows, task_rows)

    # -- public API -------------------------------------------------------

    def collect(self) -> str:
        """Run every query and return the Prometheus text payload."""
        with self._lock:
            started = time.perf_counter()
            families: list[MetricFamily] = []
            postgres_up = 1.0
            airflow_up: float | None = None
            try:
                families += self._collect_retail()
            except Exception as exc:  # noqa: BLE001 - any failure degrades safely
                postgres_up = 0.0
                self._retail_errors += 1
                self._log(f"mri-metrics: retail scrape failed: {exc}")
            if self.settings.airflow_configured:
                try:
                    families += self._collect_airflow()
                    airflow_up = 1.0
                except Exception as exc:  # noqa: BLE001
                    airflow_up = 0.0
                    self._airflow_errors += 1
                    self._log(f"mri-metrics: airflow scrape failed: {exc}")
            duration = time.perf_counter() - started
            success = 1.0 if postgres_up == 1.0 and airflow_up in (None, 1.0) else 0.0
            families += exporter_self_families(
                version=self.settings.version,
                last_scrape_success=success,
                scrape_duration_seconds=duration,
                postgres_up=postgres_up,
                airflow_up=airflow_up,
                retail_errors_total=float(self._retail_errors),
                airflow_errors_total=(
                    float(self._airflow_errors) if self.settings.airflow_configured else None
                ),
            )
            return render_text(families)


def _stderr(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def sort_families(families: Sequence[MetricFamily]) -> list[MetricFamily]:
    """Utility used by tests to keep family order deterministic."""
    return sorted(families, key=lambda item: item.name)


def as_plain_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Coerce mapping rows into plain dicts (shared by tests and tooling)."""
    return [dict(row) for row in rows]


__all__ = ["Connector", "MetricsExporter", "as_plain_rows", "default_connector", "sort_families"]
