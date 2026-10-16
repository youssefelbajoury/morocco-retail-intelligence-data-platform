"""Metric sources: SQL over the existing audit views plus pure row mappers.

Every query reads the *existing* operational surface — the ``ops`` views and
``ops.pipeline_event`` for the retail database, and Airflow's own metadata
tables for task/run outcomes. Nothing here creates a second source of truth:
the SQL is the same rollup the ``apps.observability metrics`` command reports.

The mapping functions are deliberately pure (rows in, metric families out) so
they can be unit-tested against synthetic rows without a database.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from mri.metrics.model import COUNTER, GAUGE, MetricFamily, MetricSample, family

Row = Mapping[str, Any]

# --- SQL: retail database (ops schema) -------------------------------------

PIPELINE_METRICS_SQL = """
SELECT pipeline, stage, runs, succeeded, failed, success_rate,
       rows_read, rows_written, rows_rejected,
       duration_ms_avg, duration_ms_p95
FROM ops.v_pipeline_metrics
ORDER BY pipeline, stage
"""

LATEST_RUN_SQL = """
SELECT pipeline, stage,
       extract(epoch FROM max(finished_at))::bigint AS last_run_at,
       extract(epoch FROM max(finished_at) FILTER (WHERE succeeded))::bigint AS last_success_at,
       (array_agg(succeeded ORDER BY finished_at DESC))[1] AS last_run_succeeded
FROM ops.v_run_facts
GROUP BY pipeline, stage
ORDER BY pipeline, stage
"""

FRESHNESS_SQL = """
SELECT source,
       staleness_seconds,
       extract(epoch FROM last_successful_at)::bigint AS last_success_at
FROM ops.v_freshness
ORDER BY source
"""

WAREHOUSE_RECONCILE_SQL = """
SELECT DISTINCT ON (batch_id)
       batch_id AS target_table, source AS source_table,
       status, records_read, records_written
FROM ops.pipeline_event
WHERE pipeline = 'warehouse' AND stage = 'warehouse' AND batch_id <> ''
ORDER BY batch_id, timestamp DESC, event_id DESC
"""

POSTGRES_DATABASE_SQL = """
SELECT d.datname,
       pg_database_size(d.datname)::bigint AS size_bytes,
       d.numbackends::bigint AS connections,
       CASE WHEN (d.blks_hit + d.blks_read) > 0
            THEN d.blks_hit::double precision / (d.blks_hit + d.blks_read)
            ELSE 0 END AS cache_hit_ratio,
       d.deadlocks::bigint AS deadlocks
FROM pg_stat_database d
WHERE d.datname = current_database()
"""

POSTGRES_ACTIVITY_SQL = """
SELECT COALESCE(state, 'unknown') AS state, count(*)::bigint AS connections
FROM pg_stat_activity
GROUP BY 1
ORDER BY 1
"""

# --- SQL: Airflow metadata database ----------------------------------------

AIRFLOW_DAG_RUN_SQL = """
SELECT dag_id, state, count(*)::bigint AS dag_runs
FROM dag_run
GROUP BY dag_id, state
ORDER BY dag_id, state
"""

AIRFLOW_DAG_LAST_SQL = """
SELECT dag_id, extract(epoch FROM max(start_date))::bigint AS last_run_at
FROM dag_run
GROUP BY dag_id
ORDER BY dag_id
"""

AIRFLOW_TASK_SQL = """
SELECT dag_id, state, count(*)::bigint AS task_instances
FROM task_instance
GROUP BY dag_id, state
ORDER BY dag_id, state
"""

RETAIL_QUERIES: tuple[str, ...] = (
    PIPELINE_METRICS_SQL,
    LATEST_RUN_SQL,
    FRESHNESS_SQL,
    WAREHOUSE_RECONCILE_SQL,
    POSTGRES_DATABASE_SQL,
    POSTGRES_ACTIVITY_SQL,
)

AIRFLOW_QUERIES: tuple[str, ...] = (
    AIRFLOW_DAG_RUN_SQL,
    AIRFLOW_DAG_LAST_SQL,
    AIRFLOW_TASK_SQL,
)


def _sample(name: str, value: float, **labels: str) -> MetricSample:
    return MetricSample(
        name=name,
        value=float(value),
        labels=tuple(sorted((key, str(val)) for key, val in labels.items())),
    )


def _family(name: str, help_text: str, samples: Sequence[MetricSample]) -> MetricFamily:
    return family(name, help_text, GAUGE, samples)


def pipeline_metric_families(rows: Sequence[Row]) -> list[MetricFamily]:
    """Rollups per ``(pipeline, stage)`` from ``ops.v_pipeline_metrics``."""
    definitions: tuple[tuple[str, str, str], ...] = (
        ("mri_pipeline_runs", "Total application runs per pipeline stage.", "runs"),
        ("mri_pipeline_succeeded", "Successful runs per pipeline stage.", "succeeded"),
        ("mri_pipeline_failed", "Failed runs per pipeline stage.", "failed"),
        ("mri_pipeline_success_rate", "Successful run % per pipeline stage.", "success_rate"),
        ("mri_pipeline_rows_read", "Records read per pipeline stage.", "rows_read"),
        ("mri_pipeline_rows_written", "Records written per pipeline stage.", "rows_written"),
        ("mri_pipeline_rows_rejected", "Records rejected per pipeline stage.", "rows_rejected"),
        (
            "mri_pipeline_duration_ms_avg",
            "Average run duration (ms) per pipeline stage.",
            "duration_ms_avg",
        ),
        (
            "mri_pipeline_duration_ms_p95",
            "p95 run duration (ms) per pipeline stage.",
            "duration_ms_p95",
        ),
    )
    families: list[MetricFamily] = []
    for name, help_text, column in definitions:
        samples = [
            _sample(
                name,
                float(row[column] or 0),
                pipeline=str(row["pipeline"]),
                stage=str(row["stage"]),
            )
            for row in rows
        ]
        families.append(_family(name, help_text, samples))
    return families


def latest_run_families(rows: Sequence[Row]) -> list[MetricFamily]:
    """Latest run/success timestamps and outcome per ``(pipeline, stage)``."""
    run_samples: list[MetricSample] = []
    success_samples: list[MetricSample] = []
    outcome_samples: list[MetricSample] = []
    for row in rows:
        labels = {"pipeline": str(row["pipeline"]), "stage": str(row["stage"])}
        if row["last_run_at"] is not None:
            run_samples.append(
                _sample("mri_pipeline_last_run_timestamp_seconds", row["last_run_at"], **labels)
            )
        if row["last_success_at"] is not None:
            success_samples.append(
                _sample(
                    "mri_pipeline_last_success_timestamp_seconds",
                    row["last_success_at"],
                    **labels,
                )
            )
        outcome_samples.append(
            _sample(
                "mri_pipeline_last_run_succeeded",
                1.0 if row["last_run_succeeded"] else 0.0,
                **labels,
            )
        )
    return [
        _family(
            "mri_pipeline_last_run_timestamp_seconds",
            "Unix timestamp of the most recent run per pipeline stage.",
            run_samples,
        ),
        _family(
            "mri_pipeline_last_success_timestamp_seconds",
            "Unix timestamp of the most recent successful run per pipeline stage.",
            success_samples,
        ),
        _family(
            "mri_pipeline_last_run_succeeded",
            "Whether the most recent run per pipeline stage succeeded (1) or failed (0).",
            outcome_samples,
        ),
    ]


def freshness_families(rows: Sequence[Row]) -> list[MetricFamily]:
    """Source staleness from ``ops.v_freshness``."""
    staleness = [
        _sample(
            "mri_source_freshness_seconds",
            row["staleness_seconds"] or 0,
            source=str(row["source"]),
        )
        for row in rows
    ]
    last_success = [
        _sample(
            "mri_source_last_success_timestamp_seconds",
            row["last_success_at"],
            source=str(row["source"]),
        )
        for row in rows
        if row["last_success_at"] is not None
    ]
    return [
        _family(
            "mri_source_freshness_seconds",
            "Age in seconds of the newest successful event per source.",
            staleness,
        ),
        _family(
            "mri_source_last_success_timestamp_seconds",
            "Unix timestamp of the newest successful event per source.",
            last_success,
        ),
    ]


def warehouse_reconcile_families(rows: Sequence[Row]) -> list[MetricFamily]:
    """Latest warehouse reconciliation outcome per target table."""
    balanced: list[MetricSample] = []
    row_counts: list[MetricSample] = []
    for row in rows:
        table = str(row["target_table"])
        ok = str(row["status"]) == "success"
        balanced.append(
            _sample("mri_warehouse_reconcile_balanced", 1.0 if ok else 0.0, table=table)
        )
        row_counts.append(
            _sample(
                "mri_warehouse_reconcile_rows",
                float(row["records_read"] or 0),
                table=table,
                side="silver",
            )
        )
        row_counts.append(
            _sample(
                "mri_warehouse_reconcile_rows",
                float(row["records_written"] or 0),
                table=table,
                side="warehouse",
            )
        )
    return [
        _family(
            "mri_warehouse_reconcile_balanced",
            "Warehouse reconciliation per target table (1 balanced, 0 not).",
            balanced,
        ),
        _family(
            "mri_warehouse_reconcile_rows",
            "Reconciled row counts per target table and side (silver/warehouse).",
            row_counts,
        ),
    ]


def postgres_families(
    database_rows: Sequence[Row], activity_rows: Sequence[Row]
) -> list[MetricFamily]:
    """Basic PostgreSQL health/capacity for the current database."""
    size = [
        _sample(
            "mri_postgres_database_size_bytes", row["size_bytes"] or 0, datname=str(row["datname"])
        )
        for row in database_rows
    ]
    cache = [
        _sample(
            "mri_postgres_cache_hit_ratio",
            row["cache_hit_ratio"] or 0.0,
            datname=str(row["datname"]),
        )
        for row in database_rows
    ]
    deadlocks = [
        _sample("mri_postgres_deadlocks", row["deadlocks"] or 0, datname=str(row["datname"]))
        for row in database_rows
    ]
    connections = [
        _sample("mri_postgres_connections", row["connections"] or 0, state=str(row["state"]))
        for row in activity_rows
    ]
    return [
        _family("mri_postgres_database_size_bytes", "On-disk size of the current database.", size),
        _family(
            "mri_postgres_cache_hit_ratio",
            "Buffer cache hit ratio of the current database (0..1).",
            cache,
        ),
        _family(
            "mri_postgres_deadlocks",
            "Cumulative deadlocks in the current database.",
            deadlocks,
        ),
        _family(
            "mri_postgres_connections",
            "Current PostgreSQL connections by backend state.",
            connections,
        ),
    ]


def airflow_families(
    dag_rows: Sequence[Row],
    last_rows: Sequence[Row],
    task_rows: Sequence[Row],
) -> list[MetricFamily]:
    """Airflow DAG-run and task-instance outcomes from the metadata database."""
    dag_runs = [
        _sample(
            "mri_airflow_dag_runs",
            row["dag_runs"] or 0,
            dag_id=str(row["dag_id"]),
            state=str(row["state"]),
        )
        for row in dag_rows
    ]
    last_runs = [
        _sample(
            "mri_airflow_last_dag_run_timestamp_seconds",
            row["last_run_at"],
            dag_id=str(row["dag_id"]),
        )
        for row in last_rows
        if row["last_run_at"] is not None
    ]
    tasks = [
        _sample(
            "mri_airflow_task_instances",
            row["task_instances"] or 0,
            dag_id=str(row["dag_id"]),
            state=str(row["state"]),
        )
        for row in task_rows
    ]
    return [
        _family(
            "mri_airflow_dag_runs",
            "Airflow DAG-run count by DAG and state.",
            dag_runs,
        ),
        _family(
            "mri_airflow_last_dag_run_timestamp_seconds",
            "Unix timestamp of the most recent Airflow DAG run per DAG.",
            last_runs,
        ),
        _family(
            "mri_airflow_task_instances",
            "Airflow task-instance count by DAG and state.",
            tasks,
        ),
    ]


def exporter_self_families(
    *,
    version: str,
    last_scrape_success: float,
    scrape_duration_seconds: float,
    postgres_up: float,
    airflow_up: float | None,
    retail_errors_total: float,
    airflow_errors_total: float | None,
) -> list[MetricFamily]:
    """Exporter liveness/diagnostics plus upstream ``up`` gauges."""
    families = [
        _family(
            "mri_exporter_build_info",
            "Exporter build information (constant 1, version in the label).",
            [_sample("mri_exporter_build_info", 1.0, version=version)],
        ),
        _family(
            "mri_exporter_last_scrape_success",
            "1 when the last scrape produced the full metric set, else 0.",
            [_sample("mri_exporter_last_scrape_success", last_scrape_success)],
        ),
        _family(
            "mri_exporter_scrape_duration_seconds",
            "Wall-clock duration of the last scrape in seconds.",
            [_sample("mri_exporter_scrape_duration_seconds", scrape_duration_seconds)],
        ),
        _family(
            "mri_postgres_up",
            "1 when the retail PostgreSQL database was reachable, else 0.",
            [_sample("mri_postgres_up", postgres_up)],
        ),
    ]
    error_samples = [
        _sample("mri_exporter_scrape_errors_total", retail_errors_total, source="retail")
    ]
    if airflow_errors_total is not None:
        error_samples.append(
            _sample("mri_exporter_scrape_errors_total", airflow_errors_total, source="airflow")
        )
    families.append(
        family(
            "mri_exporter_scrape_errors_total",
            "Cumulative scraper errors by source.",
            COUNTER,
            error_samples,
        )
    )
    if airflow_up is not None:
        families.append(
            _family(
                "mri_airflow_up",
                "1 when the Airflow metadata database was reachable, else 0.",
                [_sample("mri_airflow_up", airflow_up)],
            )
        )
    return families


__all__ = [
    "AIRFLOW_DAG_LAST_SQL",
    "AIRFLOW_DAG_RUN_SQL",
    "AIRFLOW_QUERIES",
    "AIRFLOW_TASK_SQL",
    "FRESHNESS_SQL",
    "LATEST_RUN_SQL",
    "PIPELINE_METRICS_SQL",
    "POSTGRES_ACTIVITY_SQL",
    "POSTGRES_DATABASE_SQL",
    "RETAIL_QUERIES",
    "WAREHOUSE_RECONCILE_SQL",
    "airflow_families",
    "exporter_self_families",
    "freshness_families",
    "latest_run_families",
    "pipeline_metric_families",
    "postgres_families",
    "warehouse_reconcile_families",
]
