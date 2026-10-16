"""Minimal Prometheus text-exposition model.

The exporter deliberately avoids a third-party client library: the exposition
format is small, stable and fully specified, so it is modelled here with three
frozen dataclasses and a deterministic renderer. Determinism (families sorted by
name, samples sorted by labels) keeps the endpoint byte-stable between scrapes,
which makes both tests and change review trivial.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

GAUGE = "gauge"
COUNTER = "counter"

_METRIC_NAME_PATTERN = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_:")
_LABEL_NAME_PATTERN = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")


@dataclass(frozen=True)
class MetricSample:
    """One time series: a value qualified by a bounded set of labels."""

    name: str
    value: float
    labels: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.name or not set(self.name) <= _METRIC_NAME_PATTERN:
            raise ValueError(f"invalid metric name: {self.name!r}")
        if self.name[0].isdigit() or self.name[0] == ":":
            raise ValueError(f"metric name must not start with {self.name[0]!r}")
        for key, _ in self.labels:
            if not key or not set(key) <= _LABEL_NAME_PATTERN or key[0].isdigit():
                raise ValueError(f"invalid label name: {key!r}")

    def formatted_labels(self) -> str:
        if not self.labels:
            return ""
        rendered = ",".join(f'{key}="{_escape_label_value(value)}"' for key, value in self.labels)
        return "{" + rendered + "}"

    def sort_key(self) -> tuple[tuple[str, str], ...]:
        return self.labels


@dataclass(frozen=True)
class MetricFamily:
    """A named group of samples sharing one ``# HELP`` and ``# TYPE``."""

    name: str
    help: str
    type: str
    samples: tuple[MetricSample, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.type not in (GAUGE, COUNTER):
            raise ValueError(f"unsupported metric type: {self.type!r}")
        for sample in self.samples:
            if sample.name != self.name:
                raise ValueError(f"sample {sample.name!r} does not belong to family {self.name!r}")


def _escape_label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _escape_help(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n")


def format_value(value: float) -> str:
    """Render a float exactly as Prometheus expects (``+Inf``/``NaN`` included)."""
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if value == int(value):
        return str(int(value))
    return repr(value)


def render_text(families: Iterable[MetricFamily]) -> str:
    """Render families to the Prometheus text exposition format.

    A duplicate ``(name, labels)`` series is a bug in the collector and raises,
    rather than emitting an ambiguous payload that Prometheus would reject.
    """
    lines: list[str] = []
    seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    for family in sorted(families, key=lambda item: item.name):
        lines.append(f"# HELP {family.name} {_escape_help(family.help)}")
        lines.append(f"# TYPE {family.name} {family.type}")
        for sample in sorted(family.samples, key=lambda item: item.sort_key()):
            key = (sample.name, sample.labels)
            if key in seen:
                raise ValueError(f"duplicate metric series: {sample.name}{sample.labels!r}")
            seen.add(key)
            lines.append(f"{sample.name}{sample.formatted_labels()} {format_value(sample.value)}")
    return "\n".join(lines) + "\n"


def family(
    name: str,
    help_text: str,
    type_: str,
    samples: Sequence[MetricSample],
) -> MetricFamily:
    return MetricFamily(name=name, help=help_text, type=type_, samples=tuple(samples))


__all__ = [
    "COUNTER",
    "GAUGE",
    "MetricFamily",
    "MetricSample",
    "family",
    "format_value",
    "render_text",
]
