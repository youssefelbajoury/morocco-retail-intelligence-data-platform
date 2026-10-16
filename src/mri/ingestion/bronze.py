"""The Bronze layer: raw source objects become quality-gated partitioned Parquet.

For every raw object the pipeline: downloads the bytes, derives full
provenance (sha256 of the exact bytes), strictly standardizes the rows against
the source contract (invalid rows are excluded and reported, never coerced),
writes deterministic partitioned Parquet parts, runs the Great Expectations
suite from :mod:`mri.ingestion.quality`, and only then decides promotion.

Failure handling:

* the RAW object is never touched (Bronze is a pure reader of Raw);
* a batch that fails quality still writes its Parquet parts and its rejected
  rows (kept for debugging) and always writes its machine-readable quality
  report, but it gets **no promotion marker** — downstream layers must only
  consume ``_promoted/`` batches;
* a batch whose standardization is fatal (missing required column) writes only
  the quality report, no parts;
* re-running is idempotent: part keys are content-derived, existing parts are
  skipped, quality is re-evaluated every run.

External responses (``raw_external/.../response.json``) are handled by the
same pipeline: :mod:`mri.ingestion.external_bronze` first flattens them into
contract-shaped JSON rows (their ``metadata.json`` provenance sidecar is
required) and the result flows through the identical standardize → Parquet →
quality → promotion path.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from mri.ingestion.bronze_schema import (
    add_provenance,
    build_parts,
    parse_rows,
    standardize,
)
from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract
from mri.ingestion.exceptions import SourceFileError, UsageError
from mri.ingestion.external_bronze import (
    external_source,
    is_external_key,
    metadata_key_for,
    normalize_external,
)
from mri.ingestion.manifest import new_run_id, now_iso, write_manifest_local
from mri.ingestion.quality import (
    REFERENCE_COLUMNS,
    QualityResult,
    failed_quality,
    quality_report_bytes,
    run_quality,
)
from mri.ingestion.storage import ObjectStore

BRONZE_OUTCOMES = ("bronzed", "already_bronzed", "failed", "planned")
PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
JSON_CONTENT_TYPE = "application/json"


@dataclass(frozen=True)
class BronzeRecord:
    """The outcome of standardizing and gating one raw batch."""

    run_id: str
    source: str
    batch: str
    raw_key: str
    contract_id: str
    contract_version: str
    outcome: str
    quality_status: str | None = None
    promoted: bool = False
    rows_in: int = 0
    rows_kept: int = 0
    rows_rejected: int = 0
    input_bytes: int = 0
    output_bytes: int = 0
    part_keys: tuple[str, ...] = ()
    quality_key: str | None = None
    promoted_key: str | None = None
    rejected_key: str | None = None
    expectations_total: int = 0
    expectations_passed: int = 0
    expectations_failed: int = 0
    reason: str | None = None
    timestamp: str = ""


@dataclass(frozen=True)
class BronzeRunResult:
    """Aggregate outcome of one Bronze run over one or more sources."""

    run_id: str
    dry_run: bool
    records: tuple[BronzeRecord, ...]
    manifest: dict[str, Any]
    manifest_path: Path | None

    @property
    def failed(self) -> tuple[BronzeRecord, ...]:
        return tuple(record for record in self.records if record.outcome == "failed")

    @property
    def counts(self) -> dict[str, int]:
        return dict(self.manifest.get("counts", {}))


def batch_id_for(raw_key: str, *, external_prefix: str | None = None) -> str:
    """Batch id for one raw object, stable across runs (content/key-derived).

    Internal objects use the raw filename stem (``sales__<hash16>``).
    External response objects all share the ``response.json`` stem, so their
    batch id is ``<source>__<sha256(full key)[:16]>`` — unique per stored
    response and identical on re-runs (idempotent part keys).
    """
    if external_prefix and is_external_key(raw_key, external_prefix):
        source = external_source(raw_key) or "external"
        digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()[:16]
        return f"{source}__{digest}"
    name = raw_key.rsplit("/", 1)[-1]
    stem = name.rsplit(".", 1)[0] if "." in name else name
    return stem or "batch"


def format_for(raw_key: str) -> str:
    name = raw_key.rsplit("/", 1)[-1]
    return name.rsplit(".", 1)[-1].lower() if "." in name else ""


def build_bronze_manifest(
    run_id: str,
    records: Sequence[BronzeRecord],
    *,
    started_at: str | None = None,
    finished_at: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Assemble the Bronze run manifest with outcome and quality counts."""
    items = [asdict(record) for record in records]
    counts = {outcome: 0 for outcome in BRONZE_OUTCOMES}
    quality = {"passed": 0, "failed": 0}
    for record in records:
        counts[record.outcome] = counts.get(record.outcome, 0) + 1
        if record.quality_status is not None:
            quality[record.quality_status] = quality.get(record.quality_status, 0) + 1
    return {
        "run_id": run_id,
        "started_at": started_at or now_iso(),
        "finished_at": finished_at or now_iso(),
        "dry_run": dry_run,
        "batch_count": len(items),
        "counts": counts,
        "quality": quality,
        "promoted": sum(1 for record in records if record.promoted),
        "records": items,
    }


class BronzePipeline:
    """Coordinates contracts, storage, standardization and the quality gate."""

    def __init__(
        self,
        store: ObjectStore,
        settings: Settings,
        contracts: Mapping[str, Contract] | None = None,
        *,
        dry_run: bool = False,
    ) -> None:
        self._store = store
        self._settings = settings
        self._contracts: dict[str, Contract] = dict(contracts or {})
        self._dry_run = dry_run
        self._raw_keys: list[str] | None = None
        self._external_keys: list[str] | None = None
        self._reference_cache: dict[str, set[str]] = {}

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    def run(
        self,
        *,
        sources: Sequence[str] | None = None,
        only_keys: Sequence[str] | None = None,
        run_id: str | None = None,
    ) -> BronzeRunResult:
        run_id = run_id or new_run_id()
        started_at = now_iso()
        groups = self._discover(sources, only_keys)
        if not self._dry_run:
            self._store.ensure_bucket(self._settings.bucket_bronze)

        records: list[BronzeRecord] = []
        for source in sorted(groups):
            contract = self._contracts[source]
            references = self._references_for(source)
            for raw_key in sorted(groups[source]):
                records.append(self._process(run_id, raw_key, source, contract, references))

        manifest = build_bronze_manifest(
            run_id, records, started_at=started_at, finished_at=now_iso(), dry_run=self._dry_run
        )
        manifest_path: Path | None = None
        if not self._dry_run:
            manifest_path = write_manifest_local(
                self._settings.bronze_manifest_dir, run_id, manifest
            )
            self._store.put_bytes(
                self._settings.bucket_bronze,
                f"{self._settings.bronze_manifest_prefix}/{run_id}.json",
                json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
                content_type=JSON_CONTENT_TYPE,
            )
        return BronzeRunResult(
            run_id=run_id,
            dry_run=self._dry_run,
            records=tuple(records),
            manifest=manifest,
            manifest_path=manifest_path,
        )

    def _list_raw(self) -> list[str]:
        if self._raw_keys is None:
            prefix = f"{self._settings.raw_prefix}/"
            self._raw_keys = self._store.list_keys(self._settings.bucket_raw, prefix)
        return self._raw_keys

    def _list_external(self) -> list[str]:
        if self._external_keys is None:
            prefix = f"{self._settings.external_prefix}/"
            self._external_keys = self._store.list_keys(self._settings.bucket_raw, prefix)
        return self._external_keys

    def _discover(
        self,
        sources: Sequence[str] | None,
        only_keys: Sequence[str] | None,
    ) -> dict[str, list[str]]:
        wanted: set[str] | None = None
        if sources:
            unknown = sorted(set(sources) - set(self._contracts))
            if unknown:
                raise UsageError(f"unknown source(s): {', '.join(unknown)}")
            wanted = set(sources)
        key_filter = set(only_keys) if only_keys is not None else None
        groups: dict[str, list[str]] = {}
        found: set[str] = set()
        for key in (*self._list_raw(), *self._list_external()):
            if is_external_key(key, self._settings.external_prefix):
                source = external_source(key)
            else:
                parts = key.split("/")
                if len(parts) != 3:
                    continue
                _, source, _filename = parts
            if source not in self._contracts:
                continue
            if wanted is not None and source not in wanted:
                continue
            if key_filter is not None and key not in key_filter:
                continue
            groups.setdefault(source, []).append(key)
            found.add(key)
        if key_filter is not None:
            missing = sorted(key_filter - found)
            if missing:
                raise UsageError(f"raw object(s) not found: {', '.join(missing)}")
        if wanted is not None:
            empty = sorted(source for source in wanted if source not in groups)
            if empty:
                raise UsageError(f"no raw objects for source(s): {', '.join(empty)}")
        return groups

    def _references_for(self, source: str) -> dict[str, set[str]]:
        references: dict[str, set[str]] = {}
        for reference, _column in REFERENCE_COLUMNS.get(source, ()):
            if reference not in self._reference_cache:
                self._reference_cache[reference] = self._load_reference_keys(reference)
            references[reference] = self._reference_cache[reference]
        return references

    def _load_reference_keys(self, source: str) -> set[str]:
        contract = self._contracts.get(source)
        if contract is None:
            return set()
        primary = contract.primary_key[0]
        keys: set[str] = set()
        for raw_key in sorted(
            key
            for key in self._list_raw()
            if key.split("/")[:2] == [self._settings.raw_prefix, source]
        ):
            try:
                frame = parse_rows(
                    self._store.get_bytes(self._settings.bucket_raw, raw_key),
                    contract,
                    format_for(raw_key),
                )
            except SourceFileError:
                continue
            if primary not in frame.columns:
                continue
            for value in frame[primary]:
                text = value if isinstance(value, str) else None
                if text is not None and text.strip():
                    keys.add(text.strip())
        return keys

    def _fetch_external_metadata(self, response_key: str, source: str) -> bytes:
        """The provenance sidecar stored next to one external response."""
        metadata_key = metadata_key_for(response_key)
        if not self._store.object_exists(self._settings.bucket_raw, metadata_key):
            raise SourceFileError(f"{source}: missing metadata sidecar {metadata_key}")
        return self._store.get_bytes(self._settings.bucket_raw, metadata_key)

    def _process(
        self,
        run_id: str,
        raw_key: str,
        source: str,
        contract: Contract,
        references: dict[str, set[str]],
    ) -> BronzeRecord:
        data = self._store.get_bytes(self._settings.bucket_raw, raw_key)
        batch_id = batch_id_for(raw_key, external_prefix=self._settings.external_prefix)
        digest = hashlib.sha256(data).hexdigest()
        stamp = now_iso()
        common: dict[str, Any] = {
            "run_id": run_id,
            "source": source,
            "batch": batch_id,
            "raw_key": raw_key,
            "contract_id": contract.id,
            "contract_version": contract.version,
            "input_bytes": len(data),
            "timestamp": stamp,
        }

        try:
            if is_external_key(raw_key, self._settings.external_prefix):
                metadata = self._fetch_external_metadata(raw_key, source)
                data = normalize_external(source, data, metadata, contract)
            parsed = parse_rows(data, contract, format_for(raw_key))
        except SourceFileError as exc:
            quality = failed_quality(
                run_id=run_id,
                source=source,
                batch_id=batch_id,
                contract=contract,
                records_checked=0,
                reason=str(exc),
            )
            quality_key = self._write_quality(run_id, source, batch_id, quality)
            return BronzeRecord(
                **common,
                outcome="failed",
                quality_status="failed",
                quality_key=quality_key,
                reason=str(exc),
            )

        batch = standardize(parsed, contract)
        if batch.fatal is not None:
            quality = failed_quality(
                run_id=run_id,
                source=source,
                batch_id=batch_id,
                contract=contract,
                records_checked=batch.rows_in,
                reason=batch.fatal,
            )
            quality_key = self._write_quality(run_id, source, batch_id, quality)
            return BronzeRecord(
                **common,
                outcome="failed",
                quality_status="failed",
                rows_in=batch.rows_in,
                quality_key=quality_key,
                reason=batch.fatal,
            )

        frame = add_provenance(
            batch.frame,
            source=source,
            source_file=raw_key,
            source_hash=digest,
            run_id=run_id,
            ingestion_timestamp=stamp,
        )
        parts = build_parts(
            frame, source=source, batch_id=batch_id, prefix=self._settings.bronze_prefix
        )
        output_bytes = sum(len(blob) for _key, blob in parts)
        base = dict(
            common,
            rows_in=batch.rows_in,
            rows_kept=batch.rows_kept,
            rows_rejected=batch.rows_rejected,
            output_bytes=output_bytes,
            part_keys=tuple(key for key, _blob in parts),
        )

        if self._dry_run:
            return BronzeRecord(**base, outcome="planned")

        missing = [
            (key, blob)
            for key, blob in parts
            if not self._store.object_exists(self._settings.bucket_bronze, key)
        ]
        for key, blob in missing:
            self._store.put_bytes(
                self._settings.bucket_bronze, key, blob, content_type=PARQUET_CONTENT_TYPE
            )

        quality = run_quality(
            batch.frame,
            contract,
            source=source,
            batch_id=batch_id,
            run_id=run_id,
            records_checked=batch.rows_in,
            records_rejected=batch.rows_rejected,
            references=references,
        )
        quality_key = self._write_quality(run_id, source, batch_id, quality)

        rejected_key: str | None = None
        if batch.rows_rejected:
            rejected_key = f"{self._settings.rejected_bronze_prefix}/{source}/{batch_id}.json"
            self._store.put_bytes(
                self._settings.bucket_bronze,
                rejected_key,
                json.dumps(
                    {
                        "run_id": run_id,
                        "source": source,
                        "batch": batch_id,
                        "raw_key": raw_key,
                        "rows_in": batch.rows_in,
                        "rows_rejected": batch.rows_rejected,
                        "issue_counts": batch.issue_counts,
                        "issues": [asdict(issue) for issue in batch.issues],
                        "rejected_rows": list(batch.rejected_rows),
                    },
                    indent=2,
                    ensure_ascii=False,
                ).encode("utf-8"),
                content_type=JSON_CONTENT_TYPE,
            )

        promoted_key: str | None = None
        promoted = quality.passed
        if promoted:
            promoted_key = f"{self._settings.promoted_prefix}/{source}/{batch_id}.json"
            self._store.put_bytes(
                self._settings.bucket_bronze,
                promoted_key,
                json.dumps(
                    {
                        "run_id": run_id,
                        "source": source,
                        "batch": batch_id,
                        "source_hash": digest,
                        "parts": [{"key": key, "bytes": len(blob)} for key, blob in parts],
                        "quality": {
                            "status": quality.status,
                            "expectations_total": quality.expectations_total,
                            "expectations_failed": quality.expectations_failed,
                            "records_checked": quality.records_checked,
                        },
                        "promoted_at": now_iso(),
                    },
                    indent=2,
                    ensure_ascii=False,
                ).encode("utf-8"),
                content_type=JSON_CONTENT_TYPE,
            )

        if not promoted:
            outcome = "failed"
        else:
            outcome = "bronzed" if missing else "already_bronzed"
        return BronzeRecord(
            **base,
            outcome=outcome,
            quality_status=quality.status,
            promoted=promoted,
            quality_key=quality_key,
            promoted_key=promoted_key,
            rejected_key=rejected_key,
            expectations_total=quality.expectations_total,
            expectations_passed=quality.expectations_passed,
            expectations_failed=quality.expectations_failed,
            reason=quality.reason,
        )

    def _write_quality(
        self, run_id: str, source: str, batch_id: str, quality: QualityResult
    ) -> str | None:
        if self._dry_run:
            return None
        key = f"{self._settings.quality_prefix}/{run_id}/{source}/{batch_id}.json"
        self._store.put_bytes(
            self._settings.bucket_bronze,
            key,
            quality_report_bytes(quality),
            content_type=JSON_CONTENT_TYPE,
        )
        return key


__all__ = [
    "BRONZE_OUTCOMES",
    "BronzePipeline",
    "BronzeRecord",
    "BronzeRunResult",
    "batch_id_for",
    "build_bronze_manifest",
    "format_for",
]
