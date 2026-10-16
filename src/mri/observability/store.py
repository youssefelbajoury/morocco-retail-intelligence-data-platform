"""Append-only JSONL event log: the canonical, always-available audit sink.

Events are appended as one JSON line each (``reports/observability/
events.jsonl`` by default). The file is plain text, so it works offline and in
dry runs, is cheap to tail, and can be bulk-loaded into the PostgreSQL ``ops``
schema by ``apps.observability sync``. ``reports/`` is gitignored — the log is
an operational artifact, never source.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path
from typing import BinaryIO

from mri.observability.events import PipelineEvent

EVENTS_FILENAME = "events.jsonl"


class EventLogError(Exception):
    """Raised when the event log cannot be written or read."""


class EventLog:
    """Append-only JSONL store rooted at one directory."""

    def __init__(self, path: Path) -> None:
        self._path = path

    @classmethod
    def resolve(cls, directory: str | Path) -> EventLog:
        return cls(Path(directory) / EVENTS_FILENAME)

    @property
    def path(self) -> Path:
        return self._path

    def append(self, event: PipelineEvent) -> None:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with self._path.open("a", encoding="utf-8") as handle:
                handle.write(event.to_json() + "\n")
        except OSError as exc:
            raise EventLogError(f"cannot append event to {self._path}: {exc}") from exc

    def count_lines(self) -> int:
        if not self._path.is_file():
            return 0
        count = 0
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                for _ in handle:
                    count += 1
        except OSError as exc:
            raise EventLogError(f"cannot read event log {self._path}: {exc}") from exc
        return count

    def read_events(self) -> list[PipelineEvent]:
        """Parse every JSON line; corrupt/blank lines are skipped."""
        events: list[PipelineEvent] = []
        if not self._path.is_file():
            return events
        try:
            with self._path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    event = _parse_line(self._path, line)
                    if event is not None:
                        events.append(event)
        except OSError as exc:
            raise EventLogError(f"cannot read event log {self._path}: {exc}") from exc
        return events

    def iter_events(self) -> Iterable[PipelineEvent]:
        return iter(self.read_events())

    def clear(self) -> None:
        try:
            if self._path.is_file():
                self._path.unlink()
        except OSError as exc:
            raise EventLogError(f"cannot clear event log {self._path}: {exc}") from exc


def _parse_line(path: Path, line: str) -> PipelineEvent | None:
    text = line.strip()
    if not text:
        return None
    try:
        return PipelineEvent.from_json_line(text)
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def load_bytes(path: Path | str) -> BinaryIO:
    """Open a JSONL file in binary mode (read by the ``sync`` loader)."""
    return Path(path).open("rb")


__all__ = ["EVENTS_FILENAME", "EventLog", "EventLogError"]
