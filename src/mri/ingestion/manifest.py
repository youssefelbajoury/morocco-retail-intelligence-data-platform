"""Run manifests: a durable JSON record of every ingestion decision.

Manifests are written locally (for inspection) and can be uploaded to the raw
bucket under ``_manifests/`` for a server-side audit trail.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from mri.ingestion.exceptions import ManifestError

OUTCOMES = ("ingested", "already_ingested", "rejected")


@dataclass(frozen=True)
class IngestRecord:
    """The result of processing a single source file."""

    run_id: str
    source: str
    original_filename: str
    sha256: str
    size_bytes: int
    format: str
    contract_id: str
    contract_version: str
    outcome: str
    row_count: int | None = None
    column_count: int | None = None
    object_key: str | None = None
    rejected_key: str | None = None
    reason: str | None = None
    timestamp: str = ""

    @property
    def ingested(self) -> bool:
        return self.outcome in {"ingested", "already_ingested"}


def now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run_id(moment: datetime | None = None) -> str:
    """Build a sortable, unique run id: ``YYYYMMDDTHHMMSSZ-<8 hex>``."""
    stamp = (moment or datetime.now(UTC)).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{secrets.token_hex(4)}"


def build_manifest(
    run_id: str,
    records: Iterable[IngestRecord],
    *,
    started_at: str | None = None,
    finished_at: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Assemble the manifest payload, including outcome counts."""
    items = [asdict(record) for record in records]
    counts = {outcome: 0 for outcome in OUTCOMES}
    for item in items:
        counts[item["outcome"]] = counts.get(item["outcome"], 0) + 1
    return {
        "run_id": run_id,
        "started_at": started_at or now_iso(),
        "finished_at": finished_at or now_iso(),
        "dry_run": dry_run,
        "file_count": len(items),
        "counts": counts,
        "records": items,
    }


def manifest_path(manifest_dir: Path, run_id: str) -> Path:
    return manifest_dir / f"{run_id}.json"


def write_manifest_local(manifest_dir: Path, run_id: str, payload: dict[str, Any]) -> Path:
    try:
        manifest_dir.mkdir(parents=True, exist_ok=True)
        target = manifest_path(manifest_dir, run_id)
        target.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    except OSError as exc:
        raise ManifestError(f"cannot write manifest {run_id}: {exc}") from exc
    return target


def read_manifest_local(manifest_dir: Path, run_id: str) -> dict[str, Any]:
    target = manifest_path(manifest_dir, run_id)
    if not target.is_file():
        raise ManifestError(f"manifest not found: {target}")
    try:
        data: dict[str, Any] = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError(f"cannot read manifest {run_id}: {exc}") from exc
    return data


def list_manifests(manifest_dir: Path) -> list[Path]:
    if not manifest_dir.is_dir():
        return []
    return sorted(manifest_dir.glob("*.json"))
