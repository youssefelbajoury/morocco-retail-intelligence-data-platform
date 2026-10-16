"""Phase 19: validate the Prometheus/Grafana assets and metric-name coverage.

These tests are intentionally offline: they parse the version-controlled
monitoring configuration and assert that every metric a dashboard or alert rule
references is actually produced by the exporter. This is the "no fabricated
panels" guard required by the phase specification.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import yaml

from mri.metrics import sources

ROOT = Path(__file__).resolve().parents[2]
MONITORING = ROOT / "monitoring"
DOCKER = ROOT / "infrastructure" / "docker"

_METRIC_REF = re.compile(r"\bmri_[a-zA-Z0-9_]+\b")


def known_metric_names() -> set[str]:
    """Every metric name the exporter can emit, derived from the mappers."""
    pipeline_row = {
        "pipeline": "p",
        "stage": "s",
        "runs": 0,
        "succeeded": 0,
        "failed": 0,
        "success_rate": 0,
        "rows_read": 0,
        "rows_written": 0,
        "rows_rejected": 0,
        "duration_ms_avg": 0,
        "duration_ms_p95": 0,
    }
    run_row = {
        "pipeline": "p",
        "stage": "s",
        "last_run_at": None,
        "last_success_at": None,
        "last_run_succeeded": False,
    }
    fresh_row = {"source": "s", "staleness_seconds": 0, "last_success_at": None}
    recon_row = {
        "target_table": "t",
        "source_table": "s",
        "status": "success",
        "records_read": 0,
        "records_written": 0,
    }
    db_row = {
        "datname": "d",
        "size_bytes": 0,
        "connections": 0,
        "cache_hit_ratio": 0.0,
        "deadlocks": 0,
    }
    act_row = {"state": "active", "connections": 0}
    dag_row = {"dag_id": "d", "state": "success", "dag_runs": 0}
    last_row = {"dag_id": "d", "last_run_at": 1}
    task_row = {"dag_id": "d", "state": "success", "task_instances": 0}

    families = [
        *sources.pipeline_metric_families([pipeline_row]),
        *sources.latest_run_families([run_row]),
        *sources.freshness_families([fresh_row]),
        *sources.warehouse_reconcile_families([recon_row]),
        *sources.postgres_families([db_row], [act_row]),
        *sources.airflow_families([dag_row], [last_row], [task_row]),
        *sources.exporter_self_families(
            version="0",
            last_scrape_success=0.0,
            scrape_duration_seconds=0.0,
            postgres_up=0.0,
            airflow_up=0.0,
            retail_errors_total=0.0,
            airflow_errors_total=0.0,
        ),
    ]
    return {family.name for family in families}


def load_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    return data


def test_prometheus_scrape_config_targets_the_exporter() -> None:
    config = load_yaml(MONITORING / "prometheus" / "prometheus.yml")
    assert config["global"]["scrape_interval"] == "30s"
    assert config["rule_files"] == ["/etc/prometheus/rules/*.yml"]
    jobs = {job["job_name"]: job for job in config["scrape_configs"]}
    assert "mri-metrics" in jobs
    targets = jobs["mri-metrics"]["static_configs"][0]["targets"]
    assert targets == ["metrics-exporter:9245"]
    assert jobs["mri-metrics"]["metrics_path"] == "/metrics"


def test_prometheus_rules_reference_only_known_metrics() -> None:
    rules = load_yaml(MONITORING / "prometheus" / "rules" / "mri.rules.yml")
    known = known_metric_names() | {"up"}
    referenced: set[str] = set()
    for group in rules["groups"]:
        for rule in group["rules"]:
            assert rule["expr"], rule
            referenced |= set(_METRIC_REF.findall(rule["expr"]))
    unknown = referenced - known
    assert not unknown, f"rules reference unknown metrics: {sorted(unknown)}"


def test_grafana_datasource_and_dashboard_provider() -> None:
    datasource = load_yaml(
        MONITORING / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
    )
    ds = datasource["datasources"][0]
    assert ds["uid"] == "mri-prometheus"
    assert ds["url"] == "http://prometheus:9090"
    assert ds["isDefault"] is True

    provider = load_yaml(MONITORING / "grafana" / "provisioning" / "dashboards" / "dashboards.yml")
    options = provider["providers"][0]["options"]
    assert options["path"] == "/var/lib/grafana/dashboards"


def test_dashboards_reference_only_known_metrics() -> None:
    known = known_metric_names()
    dashboards = sorted((MONITORING / "grafana" / "dashboards").glob("*.json"))
    assert len(dashboards) == 4
    referenced: set[str] = set()
    for path in dashboards:
        dashboard = json.loads(path.read_text(encoding="utf-8"))
        assert dashboard["uid"], path
        assert dashboard["title"], path
        assert dashboard["panels"], path
        for panel in dashboard["panels"]:
            for target in panel.get("targets", []):
                referenced |= set(_METRIC_REF.findall(target["expr"]))
    unknown = referenced - known
    assert not unknown, f"dashboards reference unknown metrics: {sorted(unknown)}"


def test_dashboard_uids_are_stable() -> None:
    uids = {
        json.loads(path.read_text(encoding="utf-8"))["uid"]
        for path in (MONITORING / "grafana" / "dashboards").glob("*.json")
    }
    assert uids == {
        "mri-pipeline-overview",
        "mri-data-quality",
        "mri-airflow-health",
        "mri-freshness-throughput",
    }


def test_compose_overlay_is_optional_and_pinned() -> None:
    overlay = load_yaml(DOCKER / "docker-compose.observability.yml")
    services = overlay["services"]
    assert set(services) == {"metrics-exporter", "prometheus", "grafana"}
    for name, service in services.items():
        assert service.get("profiles") == ["observability"], name
        image = service.get("image", "")
        assert image and not image.endswith(":latest"), name
    assert services["prometheus"]["image"] == "prom/prometheus:v3.13.4"
    assert services["grafana"]["image"] == "grafana/grafana:13.2.3"
    assert services["metrics-exporter"]["build"]["dockerfile"] == (
        "infrastructure/docker/Dockerfile.metrics"
    )
    # Prometheus retention is documented and bounded
    command = " ".join(services["prometheus"]["command"])
    assert "--storage.tsdb.retention.time=15d" in command
    assert "--storage.tsdb.retention.size=1GB" in command


def test_metrics_dockerfile_is_pinned_and_lean() -> None:
    dockerfile = (DOCKER / "Dockerfile.metrics").read_text(encoding="utf-8")
    assert "FROM python:3.12.13-slim-bookworm" in dockerfile
    assert "psycopg[binary]==3.3.6" in dockerfile
    assert "latest" not in dockerfile
    assert "USER 65534:65534" in dockerfile
