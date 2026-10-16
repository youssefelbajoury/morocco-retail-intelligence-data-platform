"""Governance tooling for the retail intelligence platform.

The current member is the contract drift detector
(:mod:`mri.governance.drift`), which keeps the YAML source contracts in sync
with the runtime implementation: the Great Expectations bronze gate
(:mod:`mri.ingestion.quality`) and the Silver DDL/loader
(:mod:`mri.silver.ddl`).
"""

from __future__ import annotations

from mri.governance.drift import (
    DRIFT_KINDS,
    DriftIssue,
    DriftReport,
    run_drift_report,
)

__all__ = ["DRIFT_KINDS", "DriftIssue", "DriftReport", "run_drift_report"]
