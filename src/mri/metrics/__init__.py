"""Phase 19 — Prometheus metrics exporter for the data platform.

A tiny, dependency-light exporter that reads the *existing* operational
surface (the ``ops`` audit views, PostgreSQL health and Airflow metadata) and
renders it in Prometheus text exposition format. It creates no new source of
truth and uses only bounded-cardinality labels.

See ``docs/observability/prometheus-grafana.md`` and
``docs/observability/metric-catalog.md``.
"""

from mri.metrics.config import MetricsSettings
from mri.metrics.exporter import MetricsExporter
from mri.metrics.model import MetricFamily, MetricSample, render_text

__all__ = ["MetricFamily", "MetricSample", "MetricsExporter", "MetricsSettings", "render_text"]
