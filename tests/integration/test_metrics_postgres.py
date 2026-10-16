"""Phase 19: integration test — the exporter against a real PostgreSQL ``ops``.

Seeds a uniquely-tagged set of events into the existing ``ops`` audit surface,
runs the exporter and asserts the emitted Prometheus text reflects them, then
removes only its own rows. Skips cleanly when PostgreSQL is unavailable.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import psycopg
import pytest

from mri.metrics.config import MetricsSettings
from mri.metrics.exporter import MetricsExporter
from mri.observability.ddl import render_ops_ddl


@pytest.fixture
def tagged_pipeline() -> Iterator[tuple[str, MetricsSettings]]:
    settings = MetricsSettings.from_env()
    try:
        connection = psycopg.connect(settings.retail_dsn, autocommit=True)
    except psycopg.Error as exc:  # pragma: no cover - environment dependent
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    tag = "mriit" + uuid.uuid4().hex[:10]
    for statement in render_ops_ddl():
        connection.execute(statement)
    rows = [
        (f"{tag}-h1", 60, f"{tag}-r1", "started", "", "", None, None, None),
        (f"{tag}-h2", 30, f"{tag}-r1", "success", tag, tag, 10, 7, 7),
        (f"{tag}-h3", 20, f"{tag}-r2", "started", "", "", None, None, None),
        (f"{tag}-h4", 10, f"{tag}-r2", "failed", "", "", None, None, None),
    ]
    for event_hash, age_s, run_id, status, source, batch_id, duration, read, written in rows:
        connection.execute(
            """
            INSERT INTO ops.pipeline_event
              (event_hash, timestamp, run_id, pipeline, stage, source,
               batch_id, status, duration_ms, records_read, records_written)
            VALUES (%s, now() - make_interval(secs => %s), %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
            """,
            (
                event_hash,
                age_s,
                run_id,
                tag,
                tag,
                source,
                batch_id,
                status,
                duration,
                read,
                written,
            ),
        )
    try:
        yield tag, settings
    finally:
        connection.execute("DELETE FROM ops.pipeline_event WHERE pipeline = %s", (tag,))
        connection.close()


def test_exporter_reflects_a_real_pipeline_run(
    tagged_pipeline: tuple[str, MetricsSettings],
) -> None:
    tag, settings = tagged_pipeline
    text = MetricsExporter(settings).collect()
    labels = f'{{pipeline="{tag}",stage="{tag}"}}'
    assert f"mri_pipeline_runs{labels} 2" in text
    assert f"mri_pipeline_succeeded{labels} 1" in text
    assert f"mri_pipeline_failed{labels} 1" in text
    assert f"mri_pipeline_success_rate{labels} 50" in text
    assert f"mri_pipeline_rows_read{labels} 7" in text
    assert f'mri_source_freshness_seconds{{source="{tag}"}}' in text
    assert "mri_exporter_last_scrape_success 1" in text
    assert "mri_postgres_up 1" in text


def test_exporter_emits_warehouse_reconcile_metrics(
    tagged_pipeline: tuple[str, MetricsSettings],
) -> None:
    _tag, settings = tagged_pipeline
    text = MetricsExporter(settings).collect()
    # The live warehouse pipeline populates one reconcile event per target table.
    assert "mri_warehouse_reconcile_balanced{" in text
    assert "mri_warehouse_reconcile_rows{" in text
