"""The ingestion pipeline: discover, validate, hash, upload and record.

The pipeline is deliberately small and deterministic. It never mutates a raw
object: files are addressed by content hash, so re-ingesting an identical file
is a no-op (``already_ingested``) and invalid files are preserved under the
rejected prefix together with a machine-readable reason sidecar.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract, load_contract
from mri.ingestion.discovery import DiscoveredFile, discover_files, sha256_file
from mri.ingestion.exceptions import SourceFileError
from mri.ingestion.manifest import (
    IngestRecord,
    build_manifest,
    new_run_id,
    now_iso,
    write_manifest_local,
)
from mri.ingestion.storage import ObjectStore
from mri.ingestion.validation import ValidationResult, validate_file

CONTENT_TYPES: dict[str, str] = {"csv": "text/csv", "json": "application/json"}


def content_addressed_key(prefix: str, source: str, filename: str, digest: str) -> str:
    """Build a deterministic raw key from source, filename stem and content hash."""
    path = Path(filename)
    return f"{prefix}/{source}/{path.stem}__{digest[:16]}{path.suffix.lower()}"


def _record(common: dict[str, Any], **overrides: Any) -> IngestRecord:
    return IngestRecord(**{**common, **overrides})


@dataclass(frozen=True)
class RunResult:
    """Aggregate outcome of one ingestion run over one or more files."""

    run_id: str
    dry_run: bool
    records: tuple[IngestRecord, ...]
    manifest: dict[str, Any]
    manifest_path: Path | None

    @property
    def ingested(self) -> tuple[IngestRecord, ...]:
        return tuple(r for r in self.records if r.outcome == "ingested")

    @property
    def rejected(self) -> tuple[IngestRecord, ...]:
        return tuple(r for r in self.records if r.outcome == "rejected")


@dataclass(frozen=True)
class FileValidationReport:
    """A discovered file paired with its validation result."""

    file: DiscoveredFile
    result: ValidationResult


class IngestionPipeline:
    """Coordinates contracts, validation and object storage for file ingestion."""

    def __init__(
        self,
        store: ObjectStore | None,
        settings: Settings,
        contracts: Mapping[str, Contract] | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        self._store = store
        self._settings = settings
        self._contracts: dict[str, Contract] = dict(contracts or {})
        self._dry_run = dry_run

    @property
    def settings(self) -> Settings:
        return self._settings

    @property
    def contracts(self) -> Mapping[str, Contract]:
        return self._contracts

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    def contract_for(self, source: str) -> Contract:
        cached = self._contracts.get(source)
        if cached is not None:
            return cached
        contract = load_contract(source, self._settings.contracts_dir)
        self._contracts[source] = contract
        return contract

    def _resolve_source(self, file: DiscoveredFile, explicit: str | None) -> str:
        source = explicit or file.source
        if source is None:
            raise SourceFileError(
                f"cannot infer source for {file.path.name!r}; pass --source explicitly"
            )
        return source

    def discover(self, path: Path, *, recursive: bool = False) -> list[DiscoveredFile]:
        return discover_files(path, self._contracts.keys(), recursive=recursive)

    def _targets(
        self,
        path: Path,
        source: str | None,
        *,
        recursive: bool,
    ) -> list[DiscoveredFile]:
        files = self.discover(path, recursive=recursive)
        targets: list[DiscoveredFile] = []
        for file in files:
            if source is None and file.source is None:
                if path.is_file():
                    raise SourceFileError(
                        f"cannot infer source for {path.name!r}; pass --source explicitly"
                    )
                continue
            targets.append(file)
        if not targets:
            raise SourceFileError(f"no ingestable (.csv/.json) files found at {path}")
        return targets

    def validate_path(
        self,
        path: Path,
        *,
        source: str | None = None,
        recursive: bool = False,
    ) -> list[FileValidationReport]:
        reports: list[FileValidationReport] = []
        for file in self._targets(path, source, recursive=recursive):
            resolved = self._resolve_source(file, source)
            contract = self.contract_for(resolved)
            reports.append(
                FileValidationReport(file=file, result=validate_file(file.path, contract, file.fmt))
            )
        return reports

    def ingest_path(
        self,
        path: Path,
        *,
        source: str | None = None,
        recursive: bool = False,
        run_id: str | None = None,
    ) -> RunResult:
        run_id = run_id or new_run_id()
        started_at = now_iso()
        files = self._targets(path, source, recursive=recursive)

        records: list[IngestRecord] = []
        for file in files:
            resolved = self._resolve_source(file, source)
            contract = self.contract_for(resolved)
            records.append(self._process_file(run_id, file, contract))

        manifest = build_manifest(
            run_id, records, started_at=started_at, finished_at=now_iso(), dry_run=self._dry_run
        )
        manifest_path: Path | None = None
        if not self._dry_run:
            manifest_path = write_manifest_local(self._settings.manifest_dir, run_id, manifest)
            self._upload_manifest(run_id, manifest)
        return RunResult(
            run_id=run_id,
            dry_run=self._dry_run,
            records=tuple(records),
            manifest=manifest,
            manifest_path=manifest_path,
        )

    def _process_file(
        self,
        run_id: str,
        file: DiscoveredFile,
        contract: Contract,
    ) -> IngestRecord:
        digest = sha256_file(file.path)
        size = file.path.stat().st_size
        result = validate_file(file.path, contract, file.fmt)
        common = {
            "run_id": run_id,
            "source": contract.id,
            "original_filename": file.path.name,
            "sha256": digest,
            "size_bytes": size,
            "format": file.fmt,
            "contract_id": contract.id,
            "contract_version": contract.version,
            "row_count": result.row_count,
            "column_count": len(result.columns),
            "timestamp": now_iso(),
        }

        if not result.ok:
            rejected_key = content_addressed_key(
                self._settings.rejected_prefix, contract.id, file.path.name, digest
            )
            if not self._dry_run:
                self._store_rejected(file.path, rejected_key, contract, digest, result)
            return _record(
                common,
                outcome="rejected",
                rejected_key=rejected_key,
                reason=result.error_summary,
            )

        object_key = content_addressed_key(
            self._settings.raw_prefix, contract.id, file.path.name, digest
        )
        if not self._dry_run and self._store is not None:
            if self._store.object_exists(self._settings.bucket_raw, object_key):
                return _record(common, outcome="already_ingested", object_key=object_key)
            self._store.ensure_bucket(self._settings.bucket_raw)
            self._store.put_file(
                self._settings.bucket_raw,
                object_key,
                file.path,
                content_type=CONTENT_TYPES.get(file.fmt, "application/octet-stream"),
                metadata={
                    "source": contract.id,
                    "contract-version": contract.version,
                    "sha256": digest,
                    "run-id": run_id,
                },
            )
        return _record(common, outcome="ingested", object_key=object_key)

    def _store_rejected(
        self,
        path: Path,
        rejected_key: str,
        contract: Contract,
        digest: str,
        result: ValidationResult,
    ) -> None:
        assert self._store is not None
        bucket = self._settings.bucket_raw
        self._store.ensure_bucket(bucket)
        self._store.put_file(
            bucket,
            rejected_key,
            path,
            content_type=CONTENT_TYPES.get(result.format, "application/octet-stream"),
            metadata={"source": contract.id, "sha256": digest, "outcome": "rejected"},
        )
        sidecar = json.dumps(
            {
                "source": contract.id,
                "contract_id": contract.id,
                "contract_version": contract.version,
                "sha256": digest,
                "issues": [
                    {
                        "code": issue.code,
                        "message": issue.message,
                        "column": issue.column,
                        "row": issue.row,
                    }
                    for issue in result.issues
                ],
            },
            indent=2,
            ensure_ascii=False,
        ).encode("utf-8")
        self._store.put_bytes(
            bucket,
            f"{rejected_key}.reason.json",
            sidecar,
            content_type="application/json",
        )

    def _upload_manifest(self, run_id: str, manifest: dict[str, Any]) -> None:
        if self._store is None:
            return
        key = f"{self._settings.manifest_prefix}/{run_id}.json"
        self._store.ensure_bucket(self._settings.bucket_raw)
        self._store.put_bytes(
            self._settings.bucket_raw,
            key,
            json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
        )
