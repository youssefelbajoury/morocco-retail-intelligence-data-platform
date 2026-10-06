"""Filesystem writer for generated datasets (CSV / Parquet) + manifest."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

import mri
from mri.synthetic.profiles import ScaleProfile
from mri.synthetic.schema import ENTITY_COLUMNS, PERIOD_END, PERIOD_START

SUPPORTED_FORMATS: tuple[str, ...] = ("csv", "parquet")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def write_entity(frame: pd.DataFrame, entity: str, base_dir: Path, fmt: str) -> Path:
    """Write one entity frame to ``<base_dir>/<entity>/<entity>.<ext>``."""
    entity_dir = base_dir / entity
    entity_dir.mkdir(parents=True, exist_ok=True)
    path = entity_dir / f"{entity}.{fmt}"
    if fmt == "csv":
        frame.to_csv(path, index=False)
    elif fmt == "parquet":
        frame.to_parquet(path, index=False)
    else:
        raise ValueError(f"unsupported format {fmt!r}; expected one of {SUPPORTED_FORMATS}")
    return path


def write_dataset(
    tables: dict[str, pd.DataFrame],
    output_root: Path | str,
    profile: ScaleProfile,
    seed: int,
    fmt: str = "csv",
) -> dict[str, Any]:
    """Write all entities under ``<output_root>/<profile>/`` and a manifest.

    The manifest carries actual row counts and a SHA-256 per file so runs with
    the same seed can be compared byte-for-byte.
    """
    if fmt not in SUPPORTED_FORMATS:
        raise ValueError(f"unsupported format {fmt!r}; expected one of {SUPPORTED_FORMATS}")
    base = Path(output_root) / profile.name
    files: dict[str, dict[str, Any]] = {}
    total = 0
    for entity, frame in tables.items():
        path = write_entity(frame, entity, base, fmt)
        files[entity] = {
            "path": path.relative_to(Path(output_root)).as_posix(),
            "rows": int(len(frame)),
            "sha256": _sha256(path),
        }
        total += int(len(frame))

    manifest: dict[str, Any] = {
        "generator": "mri.synthetic",
        "generator_version": mri.__version__,
        "profile": profile.name,
        "seed": seed,
        "format": fmt,
        "period": {"start": PERIOD_START.isoformat(), "end": PERIOD_END.isoformat()},
        "target_sales_rows": int(profile.sales_rows),
        "total_rows": total,
        "row_counts": {entity: int(len(frame)) for entity, frame in tables.items()},
        "columns": {entity: list(ENTITY_COLUMNS[entity]) for entity in tables},
        "files": files,
        "data_class": "controlled_synthetic_not_equivalent_to_a_real_enterprise",
    }
    base.mkdir(parents=True, exist_ok=True)
    manifest_path = base / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return manifest
