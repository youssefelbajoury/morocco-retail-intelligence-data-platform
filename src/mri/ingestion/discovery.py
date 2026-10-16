"""Source-file discovery, format detection and content hashing."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from mri.ingestion.exceptions import SourceFileError

EXTENSION_TO_FORMAT: dict[str, str] = {".csv": "csv", ".json": "json"}
_HASH_CHUNK = 1024 * 1024


@dataclass(frozen=True)
class DiscoveredFile:
    """One candidate source file on disk."""

    path: Path
    source: str | None
    fmt: str


def detect_format(path: Path) -> str:
    """Return ``csv`` or ``json`` for a path, or raise ``SourceFileError``."""
    fmt = EXTENSION_TO_FORMAT.get(path.suffix.lower())
    if fmt is None:
        raise SourceFileError(
            f"unsupported file type {path.suffix!r} for {path.name}; expected .csv or .json"
        )
    return fmt


def infer_source(path: Path, known_sources: Iterable[str]) -> str | None:
    """Infer a source id from a filename.

    An exact stem match wins. Otherwise the longest known source that prefixes
    the stem on a word boundary (``_`` or ``-``) is chosen, e.g. ``sales_2021``
    resolves to ``sales``.
    """
    stem = path.stem.lower()
    sources = sorted(known_sources, key=len, reverse=True)
    if stem in sources:
        return stem
    for source in sources:
        if stem.startswith(f"{source}_") or stem.startswith(f"{source}-"):
            return source
    return None


def discover_files(
    path: Path,
    known_sources: Iterable[str],
    *,
    recursive: bool = False,
) -> list[DiscoveredFile]:
    """Discover supported files at ``path`` (a file or a directory)."""
    if not path.exists():
        raise SourceFileError(f"source path does not exist: {path}")
    if path.is_file():
        candidates = [path]
    else:
        pattern = "**/*" if recursive else "*"
        candidates = [item for item in sorted(path.glob(pattern)) if item.is_file()]
    discovered: list[DiscoveredFile] = []
    for candidate in candidates:
        if candidate.suffix.lower() not in EXTENSION_TO_FORMAT:
            continue
        discovered.append(
            DiscoveredFile(
                path=candidate,
                source=infer_source(candidate, known_sources),
                fmt=detect_format(candidate),
            )
        )
    return discovered


def sha256_file(path: Path) -> str:
    """Stream a file and return its lowercase SHA-256 hex digest."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(_HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()
