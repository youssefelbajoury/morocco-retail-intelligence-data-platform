"""Command-line entry point for the metrics exporter.

Usage::

    python -m apps.metrics                       # serve /metrics on :9245
    python -m apps.metrics --port 9245 --host 0.0.0.0
    python -m apps.metrics --once                # print one scrape and exit
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from dataclasses import replace

from mri.metrics.config import MetricsSettings
from mri.metrics.exporter import MetricsExporter
from mri.metrics.server import serve

EXIT_OK = 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="apps.metrics",
        description="Expose the platform's operational metrics in Prometheus text format.",
    )
    parser.add_argument("--host", default=None, help="bind address (default: MRI_METRICS_HOST/env)")
    parser.add_argument("--port", type=int, default=None, help="bind port (default: 9245)")
    parser.add_argument(
        "--once",
        "--print",
        dest="once",
        action="store_true",
        help="perform a single scrape, print it to stdout and exit",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = MetricsSettings.from_env()
    if args.host or args.port:
        settings = replace(
            settings,
            host=args.host or settings.host,
            port=args.port or settings.port,
        )
    exporter = MetricsExporter(settings)
    if args.once:
        sys.stdout.write(exporter.collect())
        return EXIT_OK
    print(
        f"mri-metrics exporter listening on http://{settings.host}:{settings.port}/metrics "
        f"(retail + {'airflow' if settings.airflow_configured else 'no airflow'} metadata)",
        file=sys.stderr,
        flush=True,
    )
    serve(exporter, settings.host, settings.port)
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
