"""The Silver layer: promoted Bronze batches loaded into PostgreSQL.

For every ``_promoted/`` marker in the Bronze bucket the loader downloads the
Parquet parts, bulk-``COPY``s them into a constraint-free ``raw_staging``
table, upserts the batch into the matching ``silver`` table inside one
transaction, and records the attempt in ``audit.load_audit`` — which doubles
as the load registry (one ``success`` row per ``(batch_id, target_table)``).

Failure handling:

* only markers whose quality report says ``passed`` are planned (the
  promotion rule of the Bronze layer); anything else is recorded as
  ``excluded`` and never touches PostgreSQL;
* a batch that fails at any point (bad part, row-count mismatch, constraint
  violation) is fully rolled back — staging is per-batch ``TRUNCATE``d and the
  silver upsert shares the marker's transaction — and audited with
  ``status='failed'`` and the driver error message;
* re-running is idempotent: registered batches are skipped (``already_loaded``
  plus an audit ``skipped`` row), and the natural-key upsert protects against
  a lost registry; ``--force`` bypasses the skip and refreshes the registry
  row in place;
* after every run each fully-registered table is reconciled: the number of
  distinct primary keys across all its promoted Bronze batches must equal
  ``SELECT count(*)`` on the silver table.
"""

from __future__ import annotations

import io
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
from psycopg.conninfo import make_conninfo

from mri.ingestion.config import Settings
from mri.ingestion.contracts import Contract
from mri.ingestion.exceptions import SilverLoadError
from mri.ingestion.manifest import new_run_id, now_iso, write_manifest_local
from mri.ingestion.storage import ObjectStore
from mri.silver.convert import frame_to_rows
from mri.silver.ddl import (
    AUDIT_TABLE,
    SOURCE_TABLES,
    TABLE_LOAD_ORDER,
    TableSpec,
    build_table_specs,
    conflict_clause,
    ident,
    qualified,
    render_schema_ddl,
)

SILVER_OUTCOMES = ("loaded", "already_loaded", "failed", "planned", "excluded")


@dataclass(frozen=True)
class PromotedBatch:
    """A parsed ``_promoted/`` marker: which parts make up one loadable batch."""

    source: str
    batch_id: str
    marker_key: str
    part_keys: tuple[str, ...]
    part_bytes: tuple[int, ...]
    records_checked: int
    quality_status: str
    run_id: str
    promoted_at: str


@dataclass(frozen=True)
class SilverRecord:
    """The outcome of loading one promoted batch (or of rejecting its marker)."""

    run_id: str
    source: str
    batch: str
    target_table: str
    outcome: str
    rows_read: int = 0
    rows_inserted: int = 0
    rows_rejected: int = 0
    read_seconds: float = 0.0
    db_seconds: float = 0.0
    error: str | None = None
    marker_key: str | None = None


@dataclass(frozen=True)
class SilverRunResult:
    """Aggregate outcome of one Silver load run."""

    run_id: str
    dry_run: bool
    records: tuple[SilverRecord, ...]
    manifest: dict[str, Any]
    manifest_path: Path | None

    @property
    def failed(self) -> tuple[SilverRecord, ...]:
        return tuple(record for record in self.records if record.outcome == "failed")

    @property
    def counts(self) -> dict[str, int]:
        return dict(self.manifest.get("counts", {}))

    @property
    def ok(self) -> bool:
        if self.failed:
            return False
        return bool(self.manifest.get("reconciliation_balanced", True))


def conninfo_for(settings: Settings) -> str:
    """Build a psycopg connection string from the PostgreSQL settings."""
    return make_conninfo(
        host=settings.postgres_host,
        port=str(settings.postgres_port),
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
        sslmode=settings.postgres_sslmode,
        connect_timeout="5",
    )


def _parse_marker(marker_key: str, payload: Any) -> PromotedBatch:
    if not isinstance(payload, Mapping):
        raise SilverLoadError(f"marker {marker_key} is not a JSON object")
    source = str(payload.get("source", "")).strip()
    batch_id = str(payload.get("batch", "")).strip()
    if not source or not batch_id:
        raise SilverLoadError(f"marker {marker_key} is missing source/batch")
    parts = payload.get("parts")
    if not isinstance(parts, list) or not parts:
        raise SilverLoadError(f"marker {marker_key} has no parts")
    part_keys: list[str] = []
    part_bytes: list[int] = []
    for part in parts:
        if not isinstance(part, Mapping) or "key" not in part:
            raise SilverLoadError(f"marker {marker_key} has a malformed part entry")
        part_keys.append(str(part["key"]))
        part_bytes.append(int(part.get("bytes", -1)))
    quality = payload.get("quality")
    if not isinstance(quality, Mapping):
        raise SilverLoadError(f"marker {marker_key} is missing quality")
    records_checked = int(quality.get("records_checked", -1))
    if records_checked < 0:
        raise SilverLoadError(f"marker {marker_key} has an invalid records_checked")
    return PromotedBatch(
        source=source,
        batch_id=batch_id,
        marker_key=marker_key,
        part_keys=tuple(part_keys),
        part_bytes=tuple(part_bytes),
        records_checked=records_checked,
        quality_status=str(quality.get("status", "")),
        run_id=str(payload.get("run_id", "")),
        promoted_at=str(payload.get("promoted_at", "")),
    )


def discover_promoted(
    store: ObjectStore,
    settings: Settings,
) -> tuple[list[PromotedBatch], list[tuple[str, str]]]:
    """Read every promotion marker; return parsed batches and malformed markers."""
    prefix = f"{settings.promoted_prefix}/"
    batches: list[PromotedBatch] = []
    defects: list[tuple[str, str]] = []
    for marker_key in sorted(store.list_keys(settings.bucket_bronze, prefix)):
        if not marker_key.endswith(".json"):
            continue
        try:
            payload = json.loads(store.get_bytes(settings.bucket_bronze, marker_key))
            batches.append(_parse_marker(marker_key, payload))
        except (ValueError, TypeError, SilverLoadError) as exc:
            defects.append((marker_key, str(exc) or exc.__class__.__name__))
    return batches, defects


def plan_batches(
    batches: Sequence[PromotedBatch],
    defects: Sequence[tuple[str, str]],
    sources: Sequence[str] | None,
) -> tuple[list[SilverRecord], list[tuple[PromotedBatch, str]]]:
    """Split discovery results into rejected-marker records and loadable batches.

    Malformed markers and unknown sources become ``failed`` records; markers
    whose quality report did not pass become ``excluded`` records; everything
    else is returned in deterministic table-load order.
    """
    selected = set(sources) if sources is not None else None
    records: list[SilverRecord] = []
    pending: list[tuple[PromotedBatch, str]] = []
    for marker_key, reason in defects:
        records.append(
            SilverRecord(
                run_id="",
                source="",
                batch="",
                target_table="",
                outcome="failed",
                error=f"invalid promotion marker: {reason}",
                marker_key=marker_key,
            )
        )
    for batch in batches:
        if selected is not None and batch.source not in selected:
            continue
        table = SOURCE_TABLES.get(batch.source)
        if table is None:
            records.append(
                SilverRecord(
                    run_id="",
                    source=batch.source,
                    batch=batch.batch_id,
                    target_table="",
                    outcome="failed",
                    error=f"no silver table mapping for source {batch.source!r}",
                    marker_key=batch.marker_key,
                )
            )
        elif batch.quality_status != "passed":
            records.append(
                SilverRecord(
                    run_id="",
                    source=batch.source,
                    batch=batch.batch_id,
                    target_table=table,
                    outcome="excluded",
                    error=f"quality status {batch.quality_status!r} (not promoted)",
                    marker_key=batch.marker_key,
                )
            )
        else:
            pending.append((batch, table))
    pending.sort(
        key=lambda item: (TABLE_LOAD_ORDER.index(item[1]), item[0].source, item[0].batch_id)
    )
    return records, pending


def build_silver_manifest(
    run_id: str,
    records: Sequence[SilverRecord],
    *,
    started_at: str,
    finished_at: str,
    dry_run: bool,
    schemas: Mapping[str, str],
    timing: Mapping[str, float],
    tables: Mapping[str, Mapping[str, int]],
    reconciliation: Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    """Assemble the Silver run manifest, including outcome counts."""
    counts = {outcome: 0 for outcome in SILVER_OUTCOMES}
    for record in records:
        counts[record.outcome] = counts.get(record.outcome, 0) + 1
    manifest: dict[str, Any] = {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "dry_run": dry_run,
        "schemas": dict(schemas),
        "counts": counts,
        "tables": {name: dict(stats) for name, stats in tables.items()},
        "records": [asdict(record) for record in records],
        "timing": dict(timing),
    }
    if reconciliation is not None:
        manifest["reconciliation"] = [dict(entry) for entry in reconciliation]
        manifest["reconciliation_balanced"] = all(
            bool(entry.get("balanced")) for entry in reconciliation
        )
    return manifest


def _record_sort_key(record: SilverRecord) -> tuple[int, str, str]:
    if record.target_table in TABLE_LOAD_ORDER:
        order = TABLE_LOAD_ORDER.index(record.target_table)
    else:
        order = len(TABLE_LOAD_ORDER)
    return (order, record.source, record.batch)


def _table_stats(records: Sequence[SilverRecord]) -> dict[str, dict[str, int]]:
    stats: dict[str, dict[str, int]] = {}
    for record in records:
        if record.outcome != "loaded":
            continue
        entry = stats.setdefault(
            record.target_table, {"batches": 0, "rows_read": 0, "rows_inserted": 0}
        )
        entry["batches"] += 1
        entry["rows_read"] += record.rows_read
        entry["rows_inserted"] += record.rows_inserted
    return stats


def _with_run_id(run_id: str, records: Sequence[SilverRecord]) -> list[SilverRecord]:
    return [
        SilverRecord(
            run_id=run_id,
            source=record.source,
            batch=record.batch,
            target_table=record.target_table,
            outcome=record.outcome,
            rows_read=record.rows_read,
            rows_inserted=record.rows_inserted,
            rows_rejected=record.rows_rejected,
            read_seconds=record.read_seconds,
            db_seconds=record.db_seconds,
            error=record.error,
            marker_key=record.marker_key,
        )
        for record in records
    ]


class SilverLoader:
    """Loads promoted Bronze batches into the configured PostgreSQL schemas."""

    def __init__(
        self,
        store: ObjectStore,
        settings: Settings,
        contracts: Mapping[str, Contract],
        *,
        dry_run: bool = False,
    ) -> None:
        self._store = store
        self._settings = settings
        self._dry_run = dry_run
        self._specs = build_table_specs(contracts)

    @property
    def specs(self) -> Mapping[str, TableSpec]:
        return self._specs

    def run(
        self,
        *,
        sources: Sequence[str] | None = None,
        force: bool = False,
        run_id: str | None = None,
    ) -> SilverRunResult:
        """Plan and execute one load run over the current promotion markers."""
        run_id = run_id or new_run_id()
        started_at = now_iso()
        run_clock = time.perf_counter()
        if sources is not None:
            unknown = sorted(set(sources) - set(SOURCE_TABLES))
            if unknown:
                raise SilverLoadError(f"unknown source(s): {', '.join(unknown)}")

        batches, defects = discover_promoted(self._store, self._settings)
        rejected, pending = plan_batches(batches, defects, sources)
        records = _with_run_id(run_id, rejected)
        timing: dict[str, float] = {
            "schema_seconds": 0.0,
            "read_seconds": 0.0,
            "db_seconds": 0.0,
            "reconcile_seconds": 0.0,
        }
        reconciliation: list[dict[str, Any]] | None = None

        if self._dry_run:
            records.extend(
                SilverRecord(
                    run_id=run_id,
                    source=batch.source,
                    batch=batch.batch_id,
                    target_table=table,
                    outcome="planned",
                    marker_key=batch.marker_key,
                )
                for batch, table in pending
            )
        else:
            loaded, reconciliation = self._load_all(run_id, pending, batches, force, records)
            for name, value in loaded.items():
                timing[name] = timing.get(name, 0.0) + value

        records.sort(key=_record_sort_key)
        timing["total_seconds"] = round(time.perf_counter() - run_clock, 3)
        timing = {name: round(value, 3) for name, value in timing.items()}
        manifest = build_silver_manifest(
            run_id,
            records,
            started_at=started_at,
            finished_at=now_iso(),
            dry_run=self._dry_run,
            schemas={
                "staging": self._settings.staging_schema,
                "silver": self._settings.silver_schema,
                "audit": self._settings.audit_schema,
            },
            timing=timing,
            tables=_table_stats(records),
            reconciliation=reconciliation,
        )
        manifest_path = write_manifest_local(self._settings.silver_manifest_dir, run_id, manifest)
        return SilverRunResult(run_id, self._dry_run, tuple(records), manifest, manifest_path)

    def _load_all(
        self,
        run_id: str,
        pending: Sequence[tuple[PromotedBatch, str]],
        batches: Sequence[PromotedBatch],
        force: bool,
        records: list[SilverRecord],
    ) -> tuple[dict[str, float], list[dict[str, Any]]]:
        timing = {"schema_seconds": 0.0, "read_seconds": 0.0, "db_seconds": 0.0}
        try:
            connection = psycopg.connect(conninfo_for(self._settings))
        except psycopg.Error as exc:
            raise SilverLoadError(f"cannot connect to PostgreSQL: {exc}") from exc
        try:
            with connection as conn:
                timing["schema_seconds"] = self._ensure_schema(conn)
                registered = set() if force else self._loaded_set(conn)
                with conn.cursor() as cur:
                    for batch, table in pending:
                        if (batch.batch_id, table) in registered:
                            self._audit_outcome(
                                conn,
                                cur,
                                run_id=run_id,
                                batch=batch,
                                table=table,
                                started=datetime.now(UTC),
                                status="skipped",
                                rows_read=0,
                                rows_inserted=0,
                                error=None,
                            )
                            records.append(
                                SilverRecord(
                                    run_id=run_id,
                                    source=batch.source,
                                    batch=batch.batch_id,
                                    target_table=table,
                                    outcome="already_loaded",
                                    marker_key=batch.marker_key,
                                )
                            )
                            continue
                        record, read_seconds, db_seconds = self._load_one(
                            conn, cur, run_id, batch, table
                        )
                        records.append(record)
                        timing["read_seconds"] += read_seconds
                        timing["db_seconds"] += db_seconds
                reconciliation, reconcile_seconds = self._reconcile(conn, batches)
                timing["reconcile_seconds"] = reconcile_seconds
        except psycopg.Error as exc:
            raise SilverLoadError(f"PostgreSQL error: {exc}") from exc
        return timing, reconciliation

    def _ensure_schema(self, conn: Any) -> float:
        statements = render_schema_ddl(
            self._specs,
            staging_schema=self._settings.staging_schema,
            silver_schema=self._settings.silver_schema,
            audit_schema=self._settings.audit_schema,
        )
        started = time.perf_counter()
        with conn.transaction():
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
        return round(time.perf_counter() - started, 3)

    def _loaded_set(self, conn: Any) -> set[tuple[str, str]]:
        audit = qualified(self._settings.audit_schema, AUDIT_TABLE)
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT batch_id, target_table FROM {audit} WHERE status = %s", ("success",)
                )
                rows = cur.fetchall()
        return {(str(batch), str(table)) for batch, table in rows}

    def _load_one(
        self,
        conn: Any,
        cur: Any,
        run_id: str,
        batch: PromotedBatch,
        table: str,
    ) -> tuple[SilverRecord, float, float]:
        spec = self._specs[table]
        started = datetime.now(UTC)
        read_clock = time.perf_counter()
        frame: pd.DataFrame | None = None
        read_error: str | None = None
        try:
            frame = self._read_batch(batch)
        except Exception as exc:
            read_error = str(exc) or exc.__class__.__name__
        read_seconds = round(time.perf_counter() - read_clock, 3)
        if frame is None:
            self._audit_outcome(
                conn,
                cur,
                run_id=run_id,
                batch=batch,
                table=table,
                started=started,
                status="failed",
                rows_read=0,
                rows_inserted=0,
                error=read_error,
            )
            return (
                SilverRecord(
                    run_id=run_id,
                    source=batch.source,
                    batch=batch.batch_id,
                    target_table=table,
                    outcome="failed",
                    error=read_error,
                    read_seconds=read_seconds,
                    marker_key=batch.marker_key,
                ),
                read_seconds,
                0.0,
            )
        db_clock = time.perf_counter()
        load_error: str | None = None
        inserted = 0
        try:
            with conn.transaction():
                staged = self._stage(cur, frame, spec, batch)
                inserted = self._upsert(cur, spec, table, batch.source)
                if inserted != staged:
                    raise SilverLoadError(
                        f"batch {batch.batch_id}: upsert touched {inserted} rows, staged {staged}"
                    )
                self._audit_row(
                    cur,
                    run_id=run_id,
                    batch=batch,
                    table=table,
                    started=started,
                    finished=datetime.now(UTC),
                    status="success",
                    rows_read=len(frame),
                    rows_inserted=inserted,
                    error=None,
                )
        except Exception as exc:
            load_error = str(exc) or exc.__class__.__name__
        db_seconds = round(time.perf_counter() - db_clock, 3)
        if load_error is not None:
            self._audit_outcome(
                conn,
                cur,
                run_id=run_id,
                batch=batch,
                table=table,
                started=started,
                status="failed",
                rows_read=len(frame),
                rows_inserted=0,
                error=load_error,
            )
            return (
                SilverRecord(
                    run_id=run_id,
                    source=batch.source,
                    batch=batch.batch_id,
                    target_table=table,
                    outcome="failed",
                    rows_read=len(frame),
                    error=load_error,
                    read_seconds=read_seconds,
                    db_seconds=db_seconds,
                    marker_key=batch.marker_key,
                ),
                read_seconds,
                db_seconds,
            )
        return (
            SilverRecord(
                run_id=run_id,
                source=batch.source,
                batch=batch.batch_id,
                target_table=table,
                outcome="loaded",
                rows_read=len(frame),
                rows_inserted=inserted,
                read_seconds=read_seconds,
                db_seconds=db_seconds,
                marker_key=batch.marker_key,
            ),
            read_seconds,
            db_seconds,
        )

    def _read_batch(self, batch: PromotedBatch) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for key, expected in zip(batch.part_keys, batch.part_bytes, strict=True):
            blob = self._store.get_bytes(self._settings.bucket_bronze, key)
            if expected >= 0 and len(blob) != expected:
                raise SilverLoadError(
                    f"batch {batch.batch_id}: part {key} is {len(blob)} bytes, "
                    f"marker says {expected}"
                )
            frames.append(pd.read_parquet(io.BytesIO(blob)))
        frame = pd.concat(frames, ignore_index=True)
        if len(frame) != batch.records_checked:
            raise SilverLoadError(
                f"batch {batch.batch_id}: marker records_checked={batch.records_checked} "
                f"but parts contain {len(frame)} rows"
            )
        return frame

    def _stage(self, cur: Any, frame: pd.DataFrame, spec: TableSpec, batch: PromotedBatch) -> int:
        frame["batch_id"] = batch.batch_id
        known = set(spec.column_names)
        unknown = [column for column in frame.columns if column not in known]
        if unknown:
            raise SilverLoadError(
                f"batch {batch.batch_id}: bronze columns missing from table "
                f"{spec.name!r}: {unknown}"
            )
        columns = list(frame.columns)
        staging = qualified(self._settings.staging_schema, spec.name)
        cur.execute(f"TRUNCATE {staging}")
        column_list = ", ".join(ident(column) for column in columns)
        with cur.copy(f"COPY {staging} ({column_list}) FROM STDIN") as copy:
            for row in frame_to_rows(frame, columns, spec.column_types):
                copy.write_row(row)
        staged = int(cur.rowcount) if cur.rowcount >= 0 else len(frame)
        if staged != len(frame):
            raise SilverLoadError(
                f"batch {batch.batch_id}: staged {staged} rows, expected {len(frame)}"
            )
        return staged

    def _upsert(self, cur: Any, spec: TableSpec, table: str, source: str) -> int:
        columns = ", ".join(ident(name) for name in spec.column_names)
        clause = conflict_clause(spec, source)
        silver = qualified(self._settings.silver_schema, table)
        staging = qualified(self._settings.staging_schema, table)
        cur.execute(f"INSERT INTO {silver} ({columns}) SELECT {columns} FROM {staging} {clause}")
        return int(cur.rowcount)

    def _audit_outcome(
        self,
        conn: Any,
        cur: Any,
        *,
        run_id: str,
        batch: PromotedBatch,
        table: str,
        started: datetime,
        status: str,
        rows_read: int,
        rows_inserted: int,
        error: str | None,
    ) -> None:
        with conn.transaction():
            self._audit_row(
                cur,
                run_id=run_id,
                batch=batch,
                table=table,
                started=started,
                finished=datetime.now(UTC),
                status=status,
                rows_read=rows_read,
                rows_inserted=rows_inserted,
                error=error,
            )

    def _audit_row(
        self,
        cur: Any,
        *,
        run_id: str,
        batch: PromotedBatch,
        table: str,
        started: datetime,
        finished: datetime,
        status: str,
        rows_read: int,
        rows_inserted: int,
        error: str | None,
    ) -> None:
        audit = qualified(self._settings.audit_schema, AUDIT_TABLE)
        columns = (
            "run_id, batch_id, source, target_table, started_at, finished_at, "
            "status, rows_read, rows_inserted, rows_rejected, error_message"
        )
        values = (
            run_id,
            batch.batch_id,
            batch.source,
            table,
            started,
            finished,
            status,
            rows_read,
            rows_inserted,
            0,
            error,
        )
        placeholders = ", ".join(["%s"] * len(values))
        if status == "success":
            cur.execute(
                f"INSERT INTO {audit} ({columns}) VALUES ({placeholders}) "
                "ON CONFLICT (batch_id, target_table) WHERE status = 'success' "
                "DO UPDATE SET run_id = EXCLUDED.run_id, "
                "started_at = EXCLUDED.started_at, finished_at = EXCLUDED.finished_at, "
                "rows_read = EXCLUDED.rows_read, rows_inserted = EXCLUDED.rows_inserted, "
                "rows_rejected = EXCLUDED.rows_rejected, error_message = NULL",
                values,
            )
        else:
            cur.execute(f"INSERT INTO {audit} ({columns}) VALUES ({placeholders})", values)

    def _reconcile(
        self, conn: Any, batches: Sequence[PromotedBatch]
    ) -> tuple[list[dict[str, Any]], float]:
        by_table: dict[str, list[PromotedBatch]] = {}
        for batch in batches:
            table = SOURCE_TABLES.get(batch.source)
            if table is not None and batch.quality_status == "passed":
                by_table.setdefault(table, []).append(batch)
        registered = self._loaded_set(conn)
        started = time.perf_counter()
        results: list[dict[str, Any]] = []
        for table in TABLE_LOAD_ORDER:
            group = by_table.get(table)
            if not group:
                continue
            if any((batch.batch_id, table) not in registered for batch in group):
                continue
            entry: dict[str, Any] = {"target_table": table}
            try:
                bronze_keys = self._bronze_distinct_keys(table, group)
                silver_rows = self._silver_count(conn, table)
            except Exception as exc:
                entry["error"] = str(exc) or exc.__class__.__name__
                entry["balanced"] = False
                results.append(entry)
                continue
            entry["bronze_distinct_keys"] = bronze_keys
            entry["silver_rows"] = silver_rows
            entry["balanced"] = bronze_keys == silver_rows
            results.append(entry)
        return results, round(time.perf_counter() - started, 3)

    def _bronze_distinct_keys(self, table: str, batches: Sequence[PromotedBatch]) -> int:
        spec = self._specs[table]
        by_source: dict[str, list[PromotedBatch]] = {}
        for batch in batches:
            by_source.setdefault(batch.source, []).append(batch)
        total = 0
        for source, group in by_source.items():
            keys = list(spec.conflict_keys[source])
            merged = pd.concat([self._read_batch(batch) for batch in group], ignore_index=True)
            total += len(merged.drop_duplicates(subset=keys))
        return total

    def _silver_count(self, conn: Any, table: str) -> int:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT count(*) FROM {qualified(self._settings.silver_schema, table)}"
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0


__all__ = [
    "SILVER_OUTCOMES",
    "PromotedBatch",
    "SilverLoader",
    "SilverRecord",
    "SilverRunResult",
    "build_silver_manifest",
    "conninfo_for",
    "discover_promoted",
    "plan_batches",
]
