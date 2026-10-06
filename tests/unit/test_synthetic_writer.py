"""Filesystem writer + manifest tests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from mri.synthetic.profiles import get_profile
from mri.synthetic.writer import write_dataset


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_write_creates_expected_layout(tmp_path, tiny_tables):
    manifest = write_dataset(tiny_tables, tmp_path, get_profile("tiny"), 42)
    base = tmp_path / "tiny"
    assert (base / "manifest.json").exists()
    for entity, frame in tiny_tables.items():
        path = base / entity / f"{entity}.csv"
        assert path.exists()
        assert manifest["row_counts"][entity] == len(frame)
        assert manifest["files"][entity]["sha256"] == _sha256(path)
        assert manifest["columns"][entity] == list(frame.columns)


def test_manifest_records_actual_counts(tmp_path, tiny_tables):
    manifest = write_dataset(tiny_tables, tmp_path, get_profile("tiny"), 42)
    assert manifest["seed"] == 42
    assert manifest["profile"] == "tiny"
    assert manifest["target_sales_rows"] == get_profile("tiny").sales_rows
    assert manifest["row_counts"]["sales"] == len(tiny_tables["sales"])
    assert manifest["total_rows"] == sum(len(f) for f in tiny_tables.values())
    assert manifest["period"] == {"start": "2021-01-01", "end": "2024-12-31"}
    assert manifest["data_class"].startswith("controlled_synthetic")


def test_csv_output_is_byte_deterministic(tmp_path, tiny_tables):
    first = write_dataset(tiny_tables, tmp_path / "a", get_profile("tiny"), 42)
    second = write_dataset(tiny_tables, tmp_path / "b", get_profile("tiny"), 42)
    for entity in tiny_tables:
        assert first["files"][entity]["sha256"] == second["files"][entity]["sha256"]


def test_manifest_json_is_readable(tmp_path, tiny_tables):
    write_dataset(tiny_tables, tmp_path, get_profile("tiny"), 42)
    payload = json.loads((tmp_path / "tiny" / "manifest.json").read_text(encoding="utf-8"))
    assert payload["row_counts"]["sales"] == len(tiny_tables["sales"])


def test_parquet_roundtrip(tmp_path, tiny_tables):
    manifest = write_dataset(tiny_tables, tmp_path, get_profile("tiny"), 42, fmt="parquet")
    assert manifest["format"] == "parquet"
    path = tmp_path / "tiny" / "sales" / "sales.parquet"
    assert path.exists()
    loaded = pd.read_parquet(path)
    assert len(loaded) == len(tiny_tables["sales"])
    assert list(loaded.columns) == list(tiny_tables["sales"].columns)


def test_unsupported_format_raises(tmp_path, tiny_tables):
    with pytest.raises(ValueError, match="unsupported format"):
        write_dataset(tiny_tables, tmp_path, get_profile("tiny"), 42, fmt="avro")
