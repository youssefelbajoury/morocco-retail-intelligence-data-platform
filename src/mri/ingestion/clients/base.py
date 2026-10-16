"""Shared request/response types for the external API clients.

Every client builds :class:`FetchRequest` descriptions and executes them via
:meth:`ExternalClient.fetch`. The ingestion orchestrator
(:mod:`mri.ingestion.external`) never talks HTTP itself, which keeps the
storage/provenance logic testable with fake clients.
"""

from __future__ import annotations

import datetime
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@dataclass(frozen=True)
class FetchOptions:
    """Caller intent for one external ingestion run (resolved from the CLI)."""

    sources: tuple[str, ...] = ()
    date: datetime.date | None = None
    start_date: datetime.date | None = None
    end_date: datetime.date | None = None
    locations: tuple[str, ...] = ()
    barcodes: tuple[str, ...] = ()
    forecast_days: int = 7
    limit: int | None = None
    internal_products_path: Path | None = None


@dataclass(frozen=True)
class FetchRequest:
    """One logical, hashable API call plus its provenance context."""

    source: str
    data_type: str
    url: str
    params: Mapping[str, str] = field(default_factory=dict)
    payload_kind: str = "json"
    context: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FetchOutcome:
    """A validated API response ready to be persisted verbatim."""

    request: FetchRequest
    payload: Any
    raw: bytes
    status_code: int
    attempts: int
    elapsed_ms: int
    found: bool | None = None
    mapping: Mapping[str, Any] | None = None


@runtime_checkable
class ExternalClient(Protocol):
    """A source-specific client: build requests, execute them, validate payloads."""

    sources: tuple[str, ...]

    def build_requests(self, options: FetchOptions) -> list[FetchRequest]: ...

    def fetch(self, request: FetchRequest) -> FetchOutcome: ...


def selected(options: FetchOptions, sources: Sequence[str]) -> list[str]:
    """Return the intersection of the run's sources and a client's sources."""
    wanted = set(options.sources)
    return [source for source in sources if source in wanted]
