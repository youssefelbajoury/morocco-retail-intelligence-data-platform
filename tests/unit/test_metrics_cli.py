"""Phase 19: unit tests for the metrics exporter CLI."""

from __future__ import annotations

from typing import Any

import pytest

from mri.metrics import cli

PAYLOAD = 'mri_exporter_build_info{version="0.1.0"} 1\n'


class StubExporter:
    def __init__(self, settings: Any) -> None:
        self.settings = settings

    def collect(self) -> str:
        return PAYLOAD


def test_parser_defaults() -> None:
    args = cli.build_parser().parse_args([])
    assert args.host is None
    assert args.port is None
    assert args.once is False


def test_parser_accepts_flags_and_aliases() -> None:
    args = cli.build_parser().parse_args(["--host", "0.0.0.0", "--port", "1234", "--print"])
    assert args.host == "0.0.0.0"
    assert args.port == 1234
    assert args.once is True


def test_once_prints_a_single_scrape(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    monkeypatch.setattr(cli, "MetricsExporter", StubExporter)
    assert cli.main(["--once"]) == cli.EXIT_OK
    captured = capsys.readouterr()
    assert captured.out == PAYLOAD


def test_serve_is_called_with_bind_address(monkeypatch: pytest.MonkeyPatch, capsys: Any) -> None:
    calls: list[tuple[str, int]] = []
    monkeypatch.setattr(cli, "MetricsExporter", StubExporter)
    monkeypatch.setattr(cli, "serve", lambda exporter, host, port: calls.append((host, port)))
    assert cli.main(["--host", "127.0.0.1", "--port", "9245"]) == cli.EXIT_OK
    assert calls == [("127.0.0.1", 9245)]
    # the startup line goes to stderr, keeping stdout clean for scrapes
    assert capsys.readouterr().out == ""
