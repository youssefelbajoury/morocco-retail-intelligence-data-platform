"""Orchestration for external API ingestion: fetch → validate → raw storage.

Raw responses are stored **verbatim** next to a provenance sidecar under::

    <raw_external>/source=<source>/retrieved_date=YYYY-MM-DD/request_hash=<sha256>/{response,metadata}.json

``request_hash`` covers the endpoint plus sorted query parameters only, so a
rerun on the same day is an idempotent no-op (``already_fetched``) and a later
day re-fetches fresh data under a new partition. Nothing here ever writes to
the warehouse, the database or the synthetic products — this layer is raw
only.

Failures are never swallowed: a request that exhausts its retries becomes a
``failed`` manifest record (non-zero exit) while the rest of the run continues.
"""

from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

from mri.ingestion.clients.base import ExternalClient, FetchOptions, FetchRequest
from mri.ingestion.clients.fx_client import SOURCE as FX_SOURCE
from mri.ingestion.clients.product_catalog_client import SOURCE as PRODUCTS_SOURCE
from mri.ingestion.clients.weather_client import SOURCE_FORECAST, SOURCE_HISTORY
from mri.ingestion.config import Settings
from mri.ingestion.exceptions import ExternalApiError, UsageError
from mri.ingestion.manifest import new_run_id, now_iso, write_manifest_local
from mri.ingestion.storage import ObjectStore

ALL_SOURCES: tuple[str, ...] = (
    SOURCE_HISTORY,
    SOURCE_FORECAST,
    FX_SOURCE,
    PRODUCTS_SOURCE,
)

SOURCE_ALIASES: dict[str, tuple[str, ...]] = {
    "all": ALL_SOURCES,
    "weather": (SOURCE_HISTORY, SOURCE_FORECAST),
    "weather-history": (SOURCE_HISTORY,),
    "weather-forecast": (SOURCE_FORECAST,),
    "fx": (FX_SOURCE,),
    "products": (PRODUCTS_SOURCE,),
    **{source: (source,) for source in ALL_SOURCES},
}

OUTCOMES: tuple[str, ...] = ("fetched", "already_fetched", "failed", "planned")

PRODUCT_MAPPING_STATUSES = ("MATCHED", "NEW", "UNMATCHED")


def resolve_sources(names: Sequence[str]) -> tuple[str, ...]:
    """Expand CLI aliases (``all``, ``weather``, ``fx`` …) to storage source ids."""
    requested = list(names) or ["all"]
    chosen: set[str] = set()
    unknown: list[str] = []
    for name in requested:
        alias = SOURCE_ALIASES.get(name.strip().lower())
        if alias is None:
            unknown.append(name)
        else:
            chosen.update(alias)
    if unknown:
        valid = ", ".join(sorted(SOURCE_ALIASES))
        raise UsageError(f"unknown --source value(s): {', '.join(unknown)}; one of: {valid}")
    return tuple(source for source in ALL_SOURCES if source in chosen)


def read_barcodes(path: Path) -> tuple[str, ...]:
    """Read barcodes from a text file (``#`` comments and blanks allowed)."""
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise UsageError(f"cannot read barcode file {path}: {exc}") from exc
    barcodes = tuple(
        line.strip()
        for line in raw.splitlines()
        if line.strip() and not line.strip().startswith("#")
    )
    if not barcodes:
        raise UsageError(f"barcode file {path} contains no barcodes")
    return barcodes


def load_internal_barcode_index(path: Path | None) -> dict[str, str]:
    """Load ``barcode -> product_id`` links for exact product matching.

    Returns an empty index when no file is given: without internal barcodes
    every fetched product is simply ``NEW`` (or ``UNMATCHED`` when Open Food
    Facts does not know the barcode). Internal products are read-only here.
    """
    if path is None:
        return {}
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            columns = set(reader.fieldnames or [])
            if not {"barcode", "product_id"} <= columns:
                raise UsageError(
                    f"{path} must have 'barcode' and 'product_id' columns, found {sorted(columns)}"
                )
            index = {
                row["barcode"].strip(): row["product_id"].strip()
                for row in reader
                if row.get("barcode") and row.get("product_id")
            }
    except OSError as exc:
        raise UsageError(f"cannot read internal product index {path}: {exc}") from exc
    return index


def request_hash(request: FetchRequest) -> str:
    """Stable sha256 over endpoint + sorted params (retrieval date excluded)."""
    canonical = "\n".join([request.url, *(f"{k}={v}" for k, v in sorted(request.params.items()))])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def response_keys(prefix: str, source: str, day: str, digest: str) -> tuple[str, str]:
    """Return the ``(response_key, metadata_key)`` pair for one request."""
    base = f"{prefix.strip('/')}/source={source}/retrieved_date={day}/request_hash={digest}"
    return f"{base}/response.json", f"{base}/metadata.json"


def client_version() -> str:
    try:
        return version("morocco-retail-intelligence")
    except PackageNotFoundError:  # pragma: no cover - editable installs resolve
        return "0.1.0"


@dataclass(frozen=True)
class ExternalRecord:
    """The outcome of one external request inside a run."""

    run_id: str
    source: str
    data_type: str
    request_hash: str
    endpoint: str
    params: dict[str, str]
    outcome: str
    object_key: str | None = None
    metadata_key: str | None = None
    http_status: int | None = None
    attempts: int | None = None
    size_bytes: int | None = None
    sha256: str | None = None
    reason: str | None = None
    timestamp: str = ""


@dataclass(frozen=True)
class ExternalRunResult:
    """Aggregate outcome of one external ingestion run."""

    run_id: str
    dry_run: bool
    records: tuple[ExternalRecord, ...]
    manifest: dict[str, Any]
    manifest_path: Path | None

    @property
    def failed(self) -> tuple[ExternalRecord, ...]:
        return tuple(record for record in self.records if record.outcome == "failed")

    @property
    def counts(self) -> dict[str, int]:
        return dict(self.manifest.get("counts", {}))


def build_external_manifest(
    run_id: str,
    records: Sequence[ExternalRecord],
    *,
    started_at: str | None = None,
    finished_at: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Assemble the run manifest with per-outcome counts."""
    items = [asdict(record) for record in records]
    counts = {outcome: 0 for outcome in OUTCOMES}
    for item in items:
        counts[item["outcome"]] = counts.get(item["outcome"], 0) + 1
    return {
        "run_id": run_id,
        "kind": "external_api_ingestion",
        "started_at": started_at or now_iso(),
        "finished_at": finished_at or now_iso(),
        "dry_run": dry_run,
        "request_count": len(items),
        "counts": counts,
        "records": items,
    }


def build_metadata(
    *,
    request: FetchRequest,
    digest: str,
    run_id: str,
    retrieved_at: str,
    retrieved_date: str,
    user_agent: str,
    http_status: int,
    attempts: int,
    elapsed_ms: int,
    size_bytes: int,
    payload_sha256: str,
    found: bool | None = None,
    mapping: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """The provenance sidecar stored next to every raw response."""
    metadata: dict[str, Any] = {
        "schema_version": "1.0.0",
        "kind": "external_api_response",
        "run_id": run_id,
        "source": request.source,
        "data_type": request.data_type,
        "request_hash": digest,
        "retrieved_at": retrieved_at,
        "retrieved_date": retrieved_date,
        "endpoint": request.url,
        "request": {"method": "GET", "params": dict(request.params)},
        "http": {
            "status": http_status,
            "attempts": attempts,
            "elapsed_ms": elapsed_ms,
            "content_type": "application/json",
        },
        "response": {"bytes": size_bytes, "sha256": payload_sha256, "encoding": "utf-8"},
        "auth": {
            "api_key_required": bool(request.context.get("api_key_required", False)),
            "credentials_sent": False,
            "user_agent": user_agent,
        },
        "client": {"name": "mri-ingestion", "library": "httpx", "version": client_version()},
        "context": dict(request.context),
    }
    if found is not None:
        metadata["result"] = "found" if found else "not_found"
    if mapping is not None:
        metadata["mapping"] = dict(mapping)
    return metadata


class ExternalPipeline:
    """Runs external API requests and persists raw responses with provenance."""

    def __init__(
        self,
        store: ObjectStore | None,
        settings: Settings,
        clients: Sequence[ExternalClient],
        *,
        dry_run: bool = False,
        today: date | None = None,
    ) -> None:
        if not dry_run and store is None:
            raise UsageError("an ExternalPipeline without --dry-run needs an object store")
        self._store = store
        self._settings = settings
        self._clients = list(clients)
        self._dry_run = dry_run
        self._today = today
        self._bucket_ready = False

    @property
    def dry_run(self) -> bool:
        return self._dry_run

    def run(self, options: FetchOptions, run_id: str | None = None) -> ExternalRunResult:
        run_id = run_id or f"ext-{new_run_id()}"
        started_at = now_iso()
        day = (self._today or date.today()).isoformat()

        requests: list[FetchRequest] = []
        client_for: dict[str, ExternalClient] = {}
        for client in self._clients:
            for request in client.build_requests(options):
                requests.append(request)
                client_for.setdefault(request.source, client)

        records = [
            self._process(run_id, day, request, client_for[request.source]) for request in requests
        ]
        manifest = build_external_manifest(
            run_id, records, started_at=started_at, finished_at=now_iso(), dry_run=self._dry_run
        )

        manifest_path: Path | None = None
        if not self._dry_run:
            manifest_path = write_manifest_local(self._settings.manifest_dir, run_id, manifest)
            self._upload_manifest(run_id, manifest)
        return ExternalRunResult(
            run_id=run_id,
            dry_run=self._dry_run,
            records=tuple(records),
            manifest=manifest,
            manifest_path=manifest_path,
        )

    def _process(
        self,
        run_id: str,
        day: str,
        request: FetchRequest,
        client: ExternalClient,
    ) -> ExternalRecord:
        digest = request_hash(request)
        object_key, metadata_key = response_keys(
            self._settings.external_prefix, request.source, day, digest
        )
        common: dict[str, Any] = {
            "run_id": run_id,
            "source": request.source,
            "data_type": request.data_type,
            "request_hash": digest,
            "endpoint": request.url,
            "params": dict(request.params),
            "timestamp": now_iso(),
        }

        if self._dry_run:
            return ExternalRecord(
                **common,
                outcome="planned",
                object_key=object_key,
                metadata_key=metadata_key,
            )

        assert self._store is not None
        if self._store.object_exists(self._settings.bucket_raw, object_key):
            return ExternalRecord(
                **common,
                outcome="already_fetched",
                object_key=object_key,
                metadata_key=metadata_key,
            )

        try:
            outcome = client.fetch(request)
        except ExternalApiError as exc:
            code = getattr(exc, "reason", type(exc).__name__)
            return ExternalRecord(
                **common,
                outcome="failed",
                object_key=object_key,
                metadata_key=metadata_key,
                reason=f"{code}: {exc}",
            )

        payload_sha256 = hashlib.sha256(outcome.raw).hexdigest()
        metadata = build_metadata(
            request=request,
            digest=digest,
            run_id=run_id,
            retrieved_at=now_iso(),
            retrieved_date=day,
            user_agent=self._settings.external_user_agent,
            http_status=outcome.status_code,
            attempts=outcome.attempts,
            elapsed_ms=outcome.elapsed_ms,
            size_bytes=len(outcome.raw),
            payload_sha256=payload_sha256,
            found=outcome.found,
            mapping=outcome.mapping,
        )
        self._ensure_bucket()
        self._store.put_bytes(
            self._settings.bucket_raw,
            object_key,
            outcome.raw,
            content_type="application/json",
            metadata={
                "source": request.source,
                "data-type": request.data_type,
                "request-hash": digest,
                "run-id": run_id,
                "sha256": payload_sha256,
            },
        )
        self._store.put_bytes(
            self._settings.bucket_raw,
            metadata_key,
            json.dumps(metadata, indent=2, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
            metadata={"source": request.source, "run-id": run_id},
        )
        return ExternalRecord(
            **common,
            outcome="fetched",
            object_key=object_key,
            metadata_key=metadata_key,
            http_status=outcome.status_code,
            attempts=outcome.attempts,
            size_bytes=len(outcome.raw),
            sha256=payload_sha256,
        )

    def _ensure_bucket(self) -> None:
        assert self._store is not None
        if not self._bucket_ready:
            self._store.ensure_bucket(self._settings.bucket_raw)
            self._bucket_ready = True

    def _upload_manifest(self, run_id: str, manifest: dict[str, Any]) -> None:
        assert self._store is not None
        key = f"{self._settings.manifest_prefix}/{run_id}.json"
        self._ensure_bucket()
        self._store.put_bytes(
            self._settings.bucket_raw,
            key,
            json.dumps(manifest, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
            metadata={"run-id": run_id, "kind": "external_api_ingestion"},
        )
