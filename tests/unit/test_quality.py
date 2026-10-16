"""Unit tests for the Great Expectations quality gate and its report semantics.

Each test drives :func:`run_quality` over a standardized fixture batch, so the
expectations exercised here are exactly the ones the Bronze pipeline runs.
"""

from __future__ import annotations

import json
from typing import Any

from bronze_fixtures import CONTRACTS, sales_row, standardized

from mri.ingestion.bronze_schema import StandardizedBatch
from mri.ingestion.contracts import Contract
from mri.ingestion.quality import QualityResult, failed_quality, quality_report_bytes, run_quality

REFERENCES: dict[str, set[str]] = {
    "stores": {"ST01"},
    "products": {"P000001"},
    "channels": {"CH01"},
}


def _sales_frame(rows: list[dict[str, str]]) -> StandardizedBatch:
    return standardized("sales", rows)


def _run(
    batch: StandardizedBatch,
    *,
    references: dict[str, set[str]] | None = None,
    records_rejected: int = 0,
) -> QualityResult:
    contract: Contract = CONTRACTS["sales"]
    return run_quality(
        batch.frame,
        contract,
        source="sales",
        batch_id="sales__fixture",
        run_id="run-fixture",
        records_checked=batch.rows_in,
        records_rejected=records_rejected,
        references=REFERENCES if references is None else references,
    )


def _declared(result: QualityResult) -> set[str]:
    return {entry["declared"] for entry in result.failed_expectations}


def test_valid_sales_batch_passes_the_full_suite() -> None:
    result = _run(_sales_frame([sales_row(), sales_row(sale_id="S000000002")]))
    assert result.status == "passed"
    assert result.passed is True
    assert result.expectations_total >= 30
    assert result.expectations_failed == 0
    assert result.expectations_passed == result.expectations_total
    assert result.records_checked == 2
    assert result.records_rejected == 0
    assert result.failed_expectations == ()
    assert result.reason is None


def test_negative_quantity_fails_the_declared_rule() -> None:
    result = _run(_sales_frame([sales_row(), sales_row(sale_id="S2", quantity="-2")]))
    assert result.status == "failed"
    assert result.passed is False
    assert result.expectations_failed >= 1
    assert "quantity >= 1" in _declared(result)
    assert result.reason is not None
    assert "expectation(s) failed" in result.reason


def test_out_of_range_discount_fails_the_declared_rule() -> None:
    result = _run(_sales_frame([sales_row(discount_pct="1.5")]))
    assert result.status == "failed"
    assert "discount_pct between 0 and 1" in _declared(result)


def test_duplicate_primary_key_fails_uniqueness() -> None:
    result = _run(_sales_frame([sales_row(), sales_row(sale_id="S000000001")]))
    assert result.status == "failed"
    assert "primary key sale_id is unique" in _declared(result)
    unique = [
        entry
        for entry in result.failed_expectations
        if entry["declared"] == "primary key sale_id is unique"
    ]
    assert unique[0]["unexpected_count"] == 2


def test_conversion_rejected_rows_fail_even_when_gx_passes() -> None:
    batch = _sales_frame([sales_row(), sales_row(sale_id="S2", unit_price_mad="not-a-price")])
    assert batch.rows_kept == 1
    result = _run(batch, records_rejected=1)
    assert result.expectations_failed == 0
    assert result.status == "failed"
    assert result.reason is not None
    assert "1 row(s) rejected during standardization" in result.reason


def test_reference_rules_are_skipped_when_dimension_is_empty() -> None:
    result = _run(_sales_frame([sales_row()]), references={})
    assert result.status == "passed"
    assert set(result.skipped_expectations) == {
        "store_id exists in stores (no stores objects in raw)",
        "product_id exists in products (no products objects in raw)",
        "channel_id exists in channels (no channels objects in raw)",
    }


def test_reference_rule_fails_when_key_is_not_in_the_dimension() -> None:
    references = dict(REFERENCES, stores={"ST02"})
    result = _run(_sales_frame([sales_row()]), references=references)
    assert result.status == "failed"
    assert "store_id exists in stores" in _declared(result)


def test_failed_quality_report_for_fatal_batches() -> None:
    result = failed_quality(
        run_id="run-1",
        source="sales",
        batch_id="sales__fixture",
        contract=CONTRACTS["sales"],
        records_checked=0,
        reason="missing required column(s): unit_price_mad",
    )
    assert result.status == "failed"
    assert result.passed is False
    assert result.expectations_total == 0
    assert result.reason == "missing required column(s): unit_price_mad"
    payload = result.to_dict()
    assert payload["contract"] == {"id": "sales", "version": CONTRACTS["sales"].version}


def test_report_json_carries_every_required_field() -> None:
    result = _run(_sales_frame([sales_row()]))
    payload: dict[str, Any] = json.loads(quality_report_bytes(result))
    for field in (
        "run_id",
        "source",
        "batch",
        "status",
        "expectations_total",
        "expectations_passed",
        "expectations_failed",
        "records_checked",
    ):
        assert field in payload, field
    assert payload["source"] == "sales"
    assert payload["batch"] == "sales__fixture"
    assert payload["status"] == "passed"
    assert payload["promoted"] is False
    assert payload["contract"]["id"] == "sales"


def test_with_promotion_replaces_only_the_promotion_flag() -> None:
    result = _run(_sales_frame([sales_row()]))
    promoted = result.with_promotion(True)
    assert promoted.promoted is True
    assert promoted.status == result.status
    assert promoted.expectations_total == result.expectations_total
    assert result.promoted is False
