"""The Great Expectations quality gate for one standardized Bronze batch.

Every raw batch is validated against a machine-readable expectation suite that
combines three sources:

* **generic contract rules** — contract volume (row count), not-null for
  non-nullable columns, per-column types, and primary-key uniqueness;
* **declared quality rules** — the natural-language rules shipped with each
  source contract (ranges, sets, pair comparisons, referential checks), mapped
  one-to-one onto concrete GX expectations in :func:`_declared_specs`;
* **referential sets** — FK columns checked against the key sets of their
  dimension sources (skipped, with a note, when the dimension has no raw
  objects yet).

The result is a plain JSON-serializable report.  A batch is ``passed`` only
when GX reports zero failed expectations *and* standardization excluded no
rows; anything else is ``failed`` and the batch is never promoted.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from typing import Any

import great_expectations as gx
import pandas as pd

from mri.ingestion.contracts import Contract
from mri.ingestion.manifest import now_iso

MAX_FAILED_DETAILS = 20

_TYPE_LISTS: dict[str, tuple[str, ...]] = {
    "int64": ("int64",),
    "float64": ("float64",),
    "bool": ("bool",),
    "datetime64[ns]": ("datetime64[ns]",),
    "object": ("object", "str", "string"),
}

REFERENCE_COLUMNS: dict[str, tuple[tuple[str, str], ...]] = {
    "customers": (("regions", "region_id"), ("channels", "preferred_channel_id")),
    "open_meteo_forecast": (("regions", "region_id"),),
    "open_meteo_history": (("regions", "region_id"),),
    "purchases": (("stores", "store_id"), ("products", "product_id")),
    "promotions": (("products", "product_id"),),
    "sales": (
        ("stores", "store_id"),
        ("products", "product_id"),
        ("channels", "channel_id"),
    ),
    "stores": (("regions", "region_id"),),
}


@dataclass(frozen=True)
class QualitySpec:
    """One expectation plus the human/contract text it enforces."""

    declared: str
    expectation: Any


@dataclass(frozen=True)
class QualityResult:
    """Machine-readable quality report for one batch."""

    run_id: str
    source: str
    batch: str
    status: str
    expectations_total: int
    expectations_passed: int
    expectations_failed: int
    records_checked: int
    records_rejected: int = 0
    contract_id: str = ""
    contract_version: str = ""
    reason: str | None = None
    failed_expectations: tuple[dict[str, Any], ...] = ()
    skipped_expectations: tuple[str, ...] = ()
    started_at: str = ""
    finished_at: str = ""
    promoted: bool = False

    @property
    def passed(self) -> bool:
        return self.status == "passed"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        contract = {"id": payload.pop("contract_id"), "version": payload.pop("contract_version")}
        payload["contract"] = contract
        payload["failed_expectations"] = list(self.failed_expectations)
        payload["skipped_expectations"] = list(self.skipped_expectations)
        return payload

    def with_promotion(self, promoted: bool) -> QualityResult:
        return replace(self, promoted=promoted)


def failed_quality(
    *,
    run_id: str,
    source: str,
    batch_id: str,
    contract: Contract,
    records_checked: int,
    records_rejected: int = 0,
    reason: str,
) -> QualityResult:
    """A failed report for batches that never reached the GX runner (fatal parse)."""
    stamp = now_iso()
    return QualityResult(
        run_id=run_id,
        source=source,
        batch=batch_id,
        status="failed",
        expectations_total=0,
        expectations_passed=0,
        expectations_failed=0,
        records_checked=records_checked,
        records_rejected=records_rejected,
        contract_id=contract.id,
        contract_version=contract.version,
        reason=reason,
        started_at=stamp,
        finished_at=stamp,
    )


def build_specs(
    contract: Contract,
    frame: pd.DataFrame,
    references: Mapping[str, set[str]],
    skipped: list[str] | None = None,
) -> list[QualitySpec]:
    """Assemble the full expectation list for one batch (generic + declared)."""
    notes: list[str] = [] if skipped is None else skipped
    specs = _generic_specs(contract, frame)
    specs.extend(_declared_specs(contract, frame, references, notes))
    return specs


def _generic_specs(contract: Contract, frame: pd.DataFrame) -> list[QualitySpec]:
    specs: list[QualitySpec] = []
    volume = contract.raw.get("volume") or {}
    specs.append(
        QualitySpec(
            "row count within contract volume",
            gx.expectations.ExpectTableRowCountToBeBetween(
                min_value=volume.get("min_rows"), max_value=volume.get("max_rows")
            ),
        )
    )
    for column in contract.columns:
        if column.name not in frame.columns:
            continue
        if not column.nullable:
            specs.append(
                QualitySpec(
                    f"{column.name} is not null",
                    gx.expectations.ExpectColumnValuesToNotBeNull(column=column.name),
                )
            )
        type_list = _TYPE_LISTS.get(str(frame[column.name].dtype))
        if type_list is not None:
            specs.append(
                QualitySpec(
                    f"{column.name} is a valid {column.type}",
                    gx.expectations.ExpectColumnValuesToBeInTypeList(
                        column=column.name, type_list=list(type_list)
                    ),
                )
            )
    primary = contract.primary_key
    if len(primary) == 1:
        specs.append(
            QualitySpec(
                f"primary key {primary[0]} is unique",
                gx.expectations.ExpectColumnValuesToBeUnique(column=primary[0]),
            )
        )
    else:
        specs.append(
            QualitySpec(
                f"primary key ({', '.join(primary)}) is unique",
                gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=list(primary)),
            )
        )
    return specs


def _between(
    column: str,
    min_value: Any = None,
    max_value: Any = None,
    *,
    strict_min: bool = False,
) -> Any:
    return gx.expectations.ExpectColumnValuesToBeBetween(
        column=column,
        min_value=min_value,
        max_value=max_value,
        strict_min=strict_min,
    )


def _in_set(column: str, values: list[str]) -> Any:
    return gx.expectations.ExpectColumnValuesToBeInSet(column=column, value_set=sorted(values))


def _pair_ge(column_a: str, column_b: str, *, ignore_missing: bool = False) -> Any:
    # ignore_row_if="either_value_is_missing" keeps nullable pairs (external
    # weather measures) from failing just because one side is null; GX only
    # skips double-missing rows by default.
    if ignore_missing:
        return gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A=column_a,
            column_B=column_b,
            or_equal=True,
            ignore_row_if="either_value_is_missing",
        )
    return gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A=column_a, column_B=column_b, or_equal=True
    )


def _declared_specs(
    contract: Contract,
    frame: pd.DataFrame,
    references: Mapping[str, set[str]],
    skipped: list[str],
) -> list[QualitySpec]:
    """Map each contract's ``quality.declared`` line onto concrete expectations.

    Declared uniqueness/null lines are enforced by the generic primary-key and
    not-null expectations, so they are not duplicated here.
    """
    specs: list[QualitySpec] = []
    columns = set(frame.columns)

    def add(declared: str, expectation: Any) -> None:
        specs.append(QualitySpec(declared, expectation))

    def add_reference(reference: str, column: str) -> None:
        values = references.get(reference) or set()
        if column not in columns:
            skipped.append(f"{column} exists in {reference} (column absent)")
        elif not values:
            skipped.append(f"{column} exists in {reference} (no {reference} objects in raw)")
        else:
            add(f"{column} exists in {reference}", _in_set(column, sorted(values)))

    source = contract.id

    def add_weather(date_column: str) -> None:
        # datetime, not date: GX coerces date-only bounds to datetime.date and
        # pandas cannot compare datetime.date against datetime64[ns].
        add(
            f"{date_column} between 1940-01-01 and 2100-12-31",
            _between(date_column, datetime(1940, 1, 1), datetime(2100, 12, 31)),
        )
        add("latitude between -90 and 90", _between("latitude", -90, 90))
        add("longitude between -180 and 180", _between("longitude", -180, 180))
        add("temperature_2m_max between -80 and 60", _between("temperature_2m_max", -80, 60))
        add("temperature_2m_min between -80 and 60", _between("temperature_2m_min", -80, 60))
        add(
            "temperature_2m_max >= temperature_2m_min",
            _pair_ge("temperature_2m_max", "temperature_2m_min", ignore_missing=True),
        )
        add("precipitation_sum >= 0", _between("precipitation_sum", 0, None))
        add("precipitation_hours between 0 and 24", _between("precipitation_hours", 0, 24))
        add("wind_speed_10m_max >= 0", _between("wind_speed_10m_max", 0, None))
        add("weather_code between 0 and 99", _between("weather_code", 0, 99))

    if source == "calendar":
        add("month between 1 and 12", _between("month", 1, 12))
        add("quarter between 1 and 4", _between("quarter", 1, 4))
    elif source == "channels":
        add("channel_type in {physical, digital}", _in_set("channel_type", ["physical", "digital"]))
        add("demand_weight between 0 and 1", _between("demand_weight", 0, 1))
    elif source == "customers":
        add("birth_year between 1900 and 2024", _between("birth_year", 1900, 2024))
        add("gender in {F, M, U}", _in_set("gender", ["F", "M", "U"]))
        add(
            "loyalty_tier in {bronze, silver, gold, platinum}",
            _in_set("loyalty_tier", ["bronze", "gold", "platinum", "silver"]),
        )
    elif source == "frankfurter_fx":
        add(
            "date between 1999-01-01 and 2100-12-31",
            _between("date", datetime(1999, 1, 1), datetime(2100, 12, 31)),
        )
        add("base in {EUR}", _in_set("base", ["EUR"]))
        add("quote in {GBP, MAD, USD}", _in_set("quote", ["GBP", "MAD", "USD"]))
        add("rate > 0", _between("rate", 0, None, strict_min=True))
    elif source == "inventory":
        add("quantity_on_hand >= 0", _between("quantity_on_hand", 0, None))
        add("stock_value_mad >= 0", _between("stock_value_mad", 0, None))
        add(
            "stock_status in {in_stock, low, out_of_stock}",
            _in_set("stock_status", ["in_stock", "low", "out_of_stock"]),
        )
    elif source == "open_meteo_forecast":
        add_weather("forecast_date")
    elif source == "open_meteo_history":
        add_weather("observation_date")
    elif source == "openfoodfacts_products":
        add(
            "external_product_id matches ^[0-9]{8,14}$",
            gx.expectations.ExpectColumnValuesToMatchRegex(
                column="external_product_id", regex=r"^\d{8,14}$"
            ),
        )
        add(
            "mapping_status in {MATCHED, NEW, UNMATCHED}",
            _in_set("mapping_status", ["MATCHED", "NEW", "UNMATCHED"]),
        )
        add("nova_group between 1 and 4", _between("nova_group", 1, 4))
        add("energy_kcal_100g between 0 and 900", _between("energy_kcal_100g", 0, 900))
        add("fat_100g between 0 and 100", _between("fat_100g", 0, 100))
        add("sugars_100g between 0 and 100", _between("sugars_100g", 0, 100))
        add("proteins_100g between 0 and 100", _between("proteins_100g", 0, 100))
        add("salt_100g between 0 and 100", _between("salt_100g", 0, 100))
    elif source == "products":
        add("unit_cost_mad > 0", _between("unit_cost_mad", 0, None, strict_min=True))
        add("unit_price_mad >= unit_cost_mad", _pair_ge("unit_price_mad", "unit_cost_mad"))
        add("margin_pct between 0 and 1", _between("margin_pct", 0, 1))
        # datetime, not date: GX coerces date-only bounds to datetime.date and
        # pandas cannot compare datetime.date against datetime64[ns].
        add("launch_date <= 2024-12-31", _between("launch_date", None, datetime(2024, 12, 31)))
    elif source == "promotions":
        add("discount_pct between 0 and 1", _between("discount_pct", 0, 1))
        add("end_date >= start_date", _pair_ge("end_date", "start_date"))
    elif source == "purchases":
        add("quantity >= 1", _between("quantity", 1, None))
        add("unit_cost_mad > 0", _between("unit_cost_mad", 0, None, strict_min=True))
        add("total_cost_mad >= 0", _between("total_cost_mad", 0, None))
    elif source == "regions":
        add("demand_weight between 0 and 1", _between("demand_weight", 0, 1))
    elif source == "sales":
        add("quantity >= 1", _between("quantity", 1, None))
        add("hour_of_day between 8 and 23", _between("hour_of_day", 8, 23))
        add("discount_pct between 0 and 1", _between("discount_pct", 0, 1))
        add("net_amount_mad >= 0 and margin_mad >= 0", _between("net_amount_mad", 0, None))
        add("net_amount_mad >= 0 and margin_mad >= 0", _between("margin_mad", 0, None))
    elif source == "stores":
        add("size_sqm > 0", _between("size_sqm", 0, None, strict_min=True))
        add("open_date <= 2024-12-31", _between("open_date", None, datetime(2024, 12, 31)))

    for reference, column in REFERENCE_COLUMNS.get(source, ()):
        add_reference(reference, column)
    return specs


def _match_key(expectation_type: str, kwargs: Mapping[str, Any]) -> tuple[str, str]:
    for name in ("column", "column_A", "column_list"):
        if name in kwargs:
            return (expectation_type, str(kwargs[name]))
    return (expectation_type, "")


def run_quality(
    frame: pd.DataFrame,
    contract: Contract,
    *,
    source: str,
    batch_id: str,
    run_id: str,
    records_checked: int,
    records_rejected: int = 0,
    references: Mapping[str, set[str]] | None = None,
) -> QualityResult:
    """Execute the GX suite for one standardized batch and return its report."""
    started_at = now_iso()
    refs: Mapping[str, set[str]] = references or {}
    skipped: list[str] = []
    specs = build_specs(contract, frame, refs, skipped)

    context = gx.get_context(mode="ephemeral")
    try:  # the CLI is non-interactive; progress bars only add noise
        from great_expectations.data_context.types.base import ProgressBarsConfig

        context._variables.progress_bars = ProgressBarsConfig(globally=False)
    except Exception:  # pragma: no cover - GX-internal, non-fatal
        pass

    suffix = uuid.uuid4().hex[:8]
    suite = gx.ExpectationSuite(name=f"{source}__{batch_id}")
    declared_by_key: dict[tuple[str, str], str] = {}
    for spec in specs:
        suite.add_expectation(spec.expectation)
        declared_by_key[_match_key(spec.expectation.expectation_type, spec.expectation.dict())] = (
            spec.declared
        )
    context.suites.add(suite)

    data_source = context.data_sources.add_pandas(f"bronze-{suffix}")
    asset = data_source.add_dataframe_asset("batch")
    batch_definition = asset.add_batch_definition_whole_dataframe("frame")
    validation_definition = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"validate-{suffix}", data=batch_definition, suite=suite)
    )
    run = validation_definition.run(batch_parameters={"dataframe": frame})

    statistics = run.statistics
    failed: list[dict[str, Any]] = []
    for result in run.results:
        if result.success:
            continue
        config = result.expectation_config
        if config is None:  # pragma: no cover - GX always attaches a configuration
            continue
        declared = declared_by_key.get(_match_key(config.type, config.kwargs), "")
        failed.append(
            {
                "expectation": config.type,
                "declared": declared,
                "unexpected_count": result.result.get("unexpected_count"),
                "observed_value": result.result.get("observed_value"),
            }
        )
        if len(failed) >= MAX_FAILED_DETAILS:
            break

    expectations_total = int(statistics.get("evaluated_expectations", len(specs)))
    expectations_failed = int(statistics.get("unsuccessful_expectations", len(failed)))
    expectations_passed = int(statistics.get("successful_expectations", 0))
    status = "passed" if expectations_failed == 0 and records_rejected == 0 else "failed"
    reason: str | None = None
    if status == "failed":
        bits = []
        if expectations_failed:
            bits.append(f"{expectations_failed} expectation(s) failed")
        if records_rejected:
            bits.append(f"{records_rejected} row(s) rejected during standardization")
        reason = "; ".join(bits)

    return QualityResult(
        run_id=run_id,
        source=source,
        batch=batch_id,
        status=status,
        expectations_total=expectations_total,
        expectations_passed=expectations_passed,
        expectations_failed=expectations_failed,
        records_checked=records_checked,
        records_rejected=records_rejected,
        contract_id=contract.id,
        contract_version=contract.version,
        reason=reason,
        failed_expectations=tuple(failed),
        skipped_expectations=tuple(skipped),
        started_at=started_at,
        finished_at=now_iso(),
    )


def quality_report_bytes(result: QualityResult) -> bytes:
    """Serialize one quality report for object storage."""
    return json.dumps(result.to_dict(), indent=2, ensure_ascii=False).encode("utf-8")


__all__ = [
    "QualityResult",
    "QualitySpec",
    "REFERENCE_COLUMNS",
    "build_specs",
    "failed_quality",
    "quality_report_bytes",
    "run_quality",
]
