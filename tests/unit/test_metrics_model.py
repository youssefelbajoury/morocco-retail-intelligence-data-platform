"""Phase 19: unit tests for the Prometheus text-exposition model."""

from __future__ import annotations

import pytest

from mri.metrics.model import (
    COUNTER,
    GAUGE,
    MetricFamily,
    MetricSample,
    family,
    format_value,
    render_text,
)


def sample(name: str, value: float, **labels: str) -> MetricSample:
    return MetricSample(
        name=name, value=value, labels=tuple(sorted((key, str(val)) for key, val in labels.items()))
    )


def test_format_value_renders_prometheus_numbers() -> None:
    assert format_value(1.0) == "1"
    assert format_value(2.5) == "2.5"
    assert format_value(0.0) == "0"
    assert format_value(float("nan")) == "NaN"
    assert format_value(float("inf")) == "+Inf"
    assert format_value(float("-inf")) == "-Inf"


def test_metric_sample_rejects_invalid_names() -> None:
    with pytest.raises(ValueError):
        MetricSample(name="", value=1)
    with pytest.raises(ValueError):
        MetricSample(name="has space", value=1)
    with pytest.raises(ValueError):
        MetricSample(name="1leading", value=1)
    with pytest.raises(ValueError):
        MetricSample(name="ok_name", value=1, labels=(("bad name", "x"),))


def test_metric_sample_formats_and_escapes_labels() -> None:
    s = sample("mri_x", 1, pipeline="bronze", stage="bronze")
    assert s.formatted_labels() == '{pipeline="bronze",stage="bronze"}'
    escaped = MetricSample(name="mri_x", value=1, labels=(("path", 'a"b\\c\n'),))
    assert escaped.formatted_labels() == '{path="a\\"b\\\\c\\n"}'
    assert MetricSample(name="mri_x", value=1).formatted_labels() == ""


def test_metric_family_rejects_unknown_type_and_foreign_samples() -> None:
    with pytest.raises(ValueError):
        MetricFamily(name="mri_x", help="h", type="histogram", samples=())
    with pytest.raises(ValueError):
        MetricFamily(name="mri_x", help="h", type=GAUGE, samples=(sample("mri_y", 1),))


def test_render_text_is_deterministic_and_sorted() -> None:
    families = [
        family("mri_b", "help b", GAUGE, [sample("mri_b", 1, a="2"), sample("mri_b", 2, a="1")]),
        family("mri_a", "help a", COUNTER, [sample("mri_a", 3)]),
    ]
    text = render_text(families)
    lines = text.splitlines()
    assert lines[0] == "# HELP mri_a help a"
    assert lines[1] == "# TYPE mri_a counter"
    assert lines[2] == "mri_a 3"
    # mri_b samples are sorted by label tuple
    assert lines[-2:] == ['mri_b{a="1"} 2', 'mri_b{a="2"} 1']
    assert render_text(families) == text


def test_render_text_rejects_duplicate_series() -> None:
    duplicate = [
        family("mri_x", "h", GAUGE, [sample("mri_x", 1, k="v"), sample("mri_x", 2, k="v")])
    ]
    with pytest.raises(ValueError, match="duplicate metric series"):
        render_text(duplicate)


def test_render_text_escapes_help_backslashes_and_newlines() -> None:
    text = render_text([family("mri_x", "line1\nline2\\", GAUGE, [sample("mri_x", 1)])])
    assert "# HELP mri_x line1\\nline2\\\\" in text
