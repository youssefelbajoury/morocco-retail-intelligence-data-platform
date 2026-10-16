"""Phase 19: unit tests for the stdlib ``/metrics`` HTTP endpoint."""

from __future__ import annotations

import threading
import urllib.error
import urllib.request
from collections.abc import Iterator
from typing import cast

import pytest

from mri.metrics.exporter import MetricsExporter
from mri.metrics.server import METRICS_CONTENT_TYPE, build_server

PAYLOAD = 'mri_exporter_build_info{version="0.1.0"} 1\n'


class StubExporter:
    def __init__(self, payload: str) -> None:
        self._payload = payload

    def collect(self) -> str:
        return self._payload


@pytest.fixture
def base_url() -> Iterator[str]:
    exporter = cast(MetricsExporter, StubExporter(PAYLOAD))
    server = build_server(exporter, "127.0.0.1", 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host = server.server_address[0]
    port = server.server_address[1]
    assert isinstance(host, str)
    try:
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_metrics_endpoint_serves_prometheus_text(base_url: str) -> None:
    with urllib.request.urlopen(f"{base_url}/metrics", timeout=5) as response:
        assert response.status == 200
        assert response.headers["Content-Type"] == METRICS_CONTENT_TYPE
        assert response.read().decode("utf-8") == PAYLOAD


def test_health_and_index_endpoints(base_url: str) -> None:
    for path in ("/healthz", "/-/healthy"):
        with urllib.request.urlopen(f"{base_url}{path}", timeout=5) as response:
            assert response.status == 200
            assert response.read() == b"ok\n"
    with urllib.request.urlopen(f"{base_url}/", timeout=5) as response:
        assert response.status == 200
        assert b"/metrics" in response.read()


def test_unknown_path_returns_404(base_url: str) -> None:
    with pytest.raises(urllib.error.HTTPError) as excinfo:
        urllib.request.urlopen(f"{base_url}/nope", timeout=5)
    assert excinfo.value.code == 404


def test_query_string_is_ignored(base_url: str) -> None:
    with urllib.request.urlopen(f"{base_url}/metrics?foo=bar", timeout=5) as response:
        assert response.status == 200
        assert response.read().decode("utf-8") == PAYLOAD
