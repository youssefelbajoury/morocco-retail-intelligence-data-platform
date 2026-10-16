"""A minimal, stdlib-only HTTP server for the ``/metrics`` endpoint.

No web framework is imported: :class:`http.server.ThreadingHTTPServer` plus a
tiny request handler is enough for Prometheus to scrape, and it keeps both the
runtime footprint and the dependency surface at zero. ``/healthz`` supports the
container health check.
"""

from __future__ import annotations

import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from mri.metrics.exporter import MetricsExporter

METRICS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"
PLAIN_CONTENT_TYPE = "text/plain; charset=utf-8"
INDEX_BODY = b"mri-metrics exporter\nendpoints: /metrics /healthz\n"


class MetricsHTTPServer(ThreadingHTTPServer):
    """Threaded HTTP server carrying the exporter instance."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], exporter: MetricsExporter) -> None:
        super().__init__(server_address, MetricsRequestHandler)
        self.exporter = exporter


class MetricsRequestHandler(BaseHTTPRequestHandler):
    """Serves ``/metrics`` (Prometheus text) and ``/healthz`` (liveness)."""

    server_version = "mri-metrics/1.0"
    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802 - required method name
        path = self.path.split("?", 1)[0]
        if path == "/metrics":
            self._respond(200, self._exporter().collect().encode("utf-8"), METRICS_CONTENT_TYPE)
        elif path in ("/healthz", "/-/healthy"):
            self._respond(200, b"ok\n", PLAIN_CONTENT_TYPE)
        elif path == "/":
            self._respond(200, INDEX_BODY, PLAIN_CONTENT_TYPE)
        else:
            self._respond(404, b"not found\n", PLAIN_CONTENT_TYPE)

    def _exporter(self) -> MetricsExporter:
        server = self.server
        assert isinstance(server, MetricsHTTPServer)
        return server.exporter

    def _respond(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write(f"mri-metrics: {fmt % args}\n")


def build_server(exporter: MetricsExporter, host: str, port: int) -> MetricsHTTPServer:
    return MetricsHTTPServer((host, port), exporter)


def serve(exporter: MetricsExporter, host: str, port: int) -> None:
    """Block, serving scrapes until the process is stopped."""
    httpd = build_server(exporter, host, port)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        pass
    finally:
        httpd.server_close()


__all__ = [
    "METRICS_CONTENT_TYPE",
    "MetricsHTTPServer",
    "MetricsRequestHandler",
    "build_server",
    "serve",
]
