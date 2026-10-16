"""Silver layer: promoted Bronze batches loaded into PostgreSQL.

The loader discovers ``_promoted/`` markers in the Bronze bucket, bulk-loads
each batch through a ``raw_staging`` schema (PostgreSQL ``COPY``) and upserts it
into the ``silver`` schema, recording every attempt in the ``audit`` schema.
"""

from __future__ import annotations

from mri.silver.loader import (
    PromotedBatch,
    SilverLoader,
    SilverRecord,
    SilverRunResult,
    build_silver_manifest,
    conninfo_for,
    discover_promoted,
    plan_batches,
)

__all__ = [
    "PromotedBatch",
    "SilverLoader",
    "SilverRecord",
    "SilverRunResult",
    "build_silver_manifest",
    "conninfo_for",
    "discover_promoted",
    "plan_batches",
]
