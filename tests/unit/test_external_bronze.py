"""Unit tests for external Bronze: contract shape, normalization, discovery, gating.

The four external sources are exercised end-to-end against the in-memory object
store: verbatim API payloads + metadata sidecars are seeded under
``raw_external/``, flattened by :mod:`mri.ingestion.external_bronze`, and pushed
through the very same standardize → Parquet → Great Expectations → promotion
path as the internal sources.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from bronze_fixtures import (
    BRONZE_BUCKET,
    CONTRACTS,
    RAW_BUCKET,
    make_settings,
    region_row,
    seed_raw,
)

from mri.ingestion.bronze import BronzePipeline, batch_id_for
from mri.ingestion.config import Settings
from mri.ingestion.exceptions import SourceFileError
from mri.ingestion.external_bronze import (
    external_source,
    is_external_key,
    metadata_key_for,
    normalize_external,
)
from mri.ingestion.storage import InMemoryObjectStore

FORECAST = "open_meteo_forecast"
FX = "frankfurter_fx"
HISTORY = "open_meteo_history"
PRODUCTS = "openfoodfacts_products"
EXTERNAL_SOURCES = (HISTORY, FORECAST, FX, PRODUCTS)

PROVENANCE = {
    "source",
    "source_file",
    "source_hash",
    "ingestion_run_id",
    "ingestion_timestamp",
}

LOCATION = {
    "region_id": "R01",
    "region_name": "Casablanca-Settat",
    "city": "Casablanca",
    "latitude": 33.5731,
    "longitude": -7.5898,
}


# --------------------------------------------------------------------------
# payload / metadata builders (mirroring the shapes apps.ingestion external
# stores in raw_external/)
# --------------------------------------------------------------------------


def weather_payload(
    dates: list[str],
    *,
    temperature_max: list[float | None] | None = None,
) -> dict[str, Any]:
    count = len(dates)
    return {
        "latitude": 33.57,
        "longitude": -7.59,
        "timezone": "Africa/Casablanca",
        "daily_units": {"temperature_2m_max": "°C"},
        "daily": {
            "time": dates,
            "weather_code": [3] * count,
            "temperature_2m_max": temperature_max
            if temperature_max is not None
            else [28.4] * count,
            "temperature_2m_min": [20.8] * count,
            "precipitation_sum": [0.0] * count,
            "precipitation_hours": [0.0] * count,
            "wind_speed_10m_max": [18.5] * count,
        },
    }


def weather_metadata(source: str, *, data_type: str, day: str = "2026-10-07") -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "kind": "external_api_response",
        "source": source,
        "data_type": data_type,
        "retrieved_at": f"{day}T12:00:00+00:00",
        "retrieved_date": day,
        "context": {
            "provider": "Open-Meteo",
            "data_type": data_type,
            "timezone": "Africa/Casablanca",
            "location": dict(LOCATION),
        },
    }


def fx_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return rows


def fx_row(rate: Any = 11.2003, **overrides: Any) -> dict[str, Any]:
    row = {"date": "2026-10-06", "base": "EUR", "quote": "MAD", "rate": rate}
    row.update(overrides)
    return row


def fx_metadata(day: str = "2026-10-07") -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "kind": "external_api_response",
        "source": FX,
        "data_type": "fx_rates",
        "retrieved_at": f"{day}T12:00:00+00:00",
        "retrieved_date": day,
        "context": {"provider": "Frankfurter (ECB reference rates)", "base": "EUR"},
    }


def product_payload(code: str, *, found: bool = True) -> dict[str, Any]:
    if not found:
        return {
            "code": code,
            "status": "failure",
            "result": {"id": "product_not_found"},
            "errors": [],
            "product": {},
        }
    return {
        "code": code,
        "status": "success",
        "result": {"id": "product_found"},
        "errors": [],
        "product": {
            "code": code,
            "product_name": "Nutella",
            "brands": "Nutella, Ferrero",
            "quantity": "400 g e",
            "categories": "Spreads",
            "nutriscore_grade": "e",
            "nova_group": 4,
            "image_url": "https://images.openfoodfacts.org/images/products/front.jpg",
            "countries": "France",
            "lang": "fr",
            "ingredients_text": "Sucre, huile de palme",
            "allergens": "lait, soja",
            "product_quantity": 400,
            "nutriments": {
                "energy-kcal_100g": 539,
                "fat_100g": 30.9,
                "sugars_100g": 56.3,
                "proteins_100g": 6.3,
                "salt_100g": 0.107,
            },
        },
    }


def product_metadata(
    code: str, *, found: bool = True, mapping: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "kind": "external_api_response",
        "source": PRODUCTS,
        "data_type": "product_catalog",
        "retrieved_at": "2026-10-07T12:00:00+00:00",
        "retrieved_date": "2026-10-07",
        "result": "found" if found else "not_found",
        "context": {"provider": "Open Food Facts", "barcode": code},
        "mapping": mapping
        or {
            "external_product_id": code,
            "internal_product_id": None,
            "source": PRODUCTS,
            "mapping_status": "NEW" if found else "UNMATCHED",
        },
    }


def seed_external(
    store: Any,
    settings: Settings,
    source: str,
    payload: Any,
    metadata: dict[str, Any] | None = None,
    *,
    digest: str | None = None,
    day: str = "2026-10-07",
    with_metadata: bool = True,
) -> str:
    """Store one response (+ sidecar) exactly like the external pipeline does."""
    request_hash = digest or f"{source:0<64}"[:64]
    base = (
        f"{settings.external_prefix}/source={source}"
        f"/retrieved_date={day}/request_hash={request_hash}"
    )
    response_key = f"{base}/response.json"
    store.put_bytes(settings.bucket_raw, response_key, json.dumps(payload).encode("utf-8"))
    if with_metadata:
        sidecar = metadata
        if sidecar is None:
            sidecar = weather_metadata(source, data_type="historical")
        store.put_bytes(
            settings.bucket_raw,
            f"{base}/metadata.json",
            json.dumps(sidecar).encode("utf-8"),
        )
    return response_key


def normalize(source: str, payload: Any, metadata: Any, *, day: str = "2026-10-07") -> list[Any]:
    normalized = normalize_external(
        source,
        json.dumps(payload).encode("utf-8"),
        json.dumps(metadata).encode("utf-8"),
        CONTRACTS[source],
    )
    rows: list[Any] = json.loads(normalized)
    return rows


def _pipeline(
    tmp_path: Path, *, dry_run: bool = False
) -> tuple[BronzePipeline, Settings, InMemoryObjectStore]:
    settings = make_settings(tmp_path)
    store = InMemoryObjectStore()
    return BronzePipeline(store, settings, CONTRACTS, dry_run=dry_run), settings, store


def _payload(store: InMemoryObjectStore, key: str | None) -> dict[str, Any]:
    assert key is not None
    payload: dict[str, Any] = json.loads(store.get_bytes(BRONZE_BUCKET, key))
    return payload


def _snapshot(store: InMemoryObjectStore, bucket: str) -> dict[str, bytes]:
    return {key: store.get_bytes(bucket, key) for key in store.list_keys(bucket)}


# --------------------------------------------------------------------------
# contracts
# --------------------------------------------------------------------------


def test_external_contracts_load_with_json_format_and_expected_keys() -> None:
    assert set(EXTERNAL_SOURCES) <= set(CONTRACTS)
    history = CONTRACTS[HISTORY]
    assert history.format_types == ("json",)
    assert history.primary_key == ("region_id", "observation_date")
    assert history.column_map()["temperature_2m_max"].nullable is True
    assert history.column_map()["region_id"].nullable is False

    forecast = CONTRACTS[FORECAST]
    assert forecast.primary_key == ("region_id", "forecast_date")
    assert "observation_date" not in forecast.column_names

    fx = CONTRACTS[FX]
    assert fx.primary_key == ("date", "base", "quote")
    assert fx.column_names == ("date", "base", "quote", "rate")

    products = CONTRACTS[PRODUCTS]
    assert products.primary_key == ("external_product_id",)
    assert products.column_map()["salt_100g"].nullable is True
    assert products.column_map()["mapping_status"].nullable is False
    for contract in (history, forecast, fx, products):
        assert contract.raw["volume"]["min_rows"] == 1
        assert contract.raw["quality"]["declared"]


# --------------------------------------------------------------------------
# normalization
# --------------------------------------------------------------------------


def test_normalize_history_payload_extracts_region_context_and_series() -> None:
    payload = weather_payload(["2026-09-27", "2026-09-28"])
    rows = normalize(HISTORY, payload, weather_metadata(HISTORY, data_type="historical"))

    assert len(rows) == 2
    assert set(rows[0]) == set(CONTRACTS[HISTORY].column_names)
    assert rows[0]["region_id"] == "R01"
    assert rows[0]["region_name"] == "Casablanca-Settat"
    assert rows[0]["city"] == "Casablanca"
    assert rows[0]["timezone"] == "Africa/Casablanca"
    assert rows[0]["observation_date"] == "2026-09-27"
    assert rows[1]["observation_date"] == "2026-09-28"
    assert rows[0]["data_type"] == "historical"
    assert rows[0]["retrieval_time"] == "2026-10-07T12:00:00+00:00"
    assert rows[0]["temperature_2m_max"] == 28.4
    assert rows[0]["weather_code"] == 3
    assert rows[0]["latitude"] == 33.57


def test_normalize_forecast_payload_uses_forecast_date_column() -> None:
    rows = normalize(
        FORECAST,
        weather_payload(["2026-10-07"]),
        weather_metadata(FORECAST, data_type="forecast"),
    )

    assert len(rows) == 1
    assert set(rows[0]) == set(CONTRACTS[FORECAST].column_names)
    assert rows[0]["forecast_date"] == "2026-10-07"
    assert "observation_date" not in rows[0]
    assert rows[0]["data_type"] == "forecast"


def test_normalize_fx_payload_keeps_only_contract_columns() -> None:
    rows = normalize(
        FX,
        fx_payload([fx_row(), fx_row(quote="USD", rate=1.1236)]),
        fx_metadata(),
    )

    assert len(rows) == 2
    assert set(rows[0]) == {"date", "base", "quote", "rate"}
    assert rows[0] == {"date": "2026-10-06", "base": "EUR", "quote": "MAD", "rate": 11.2003}
    assert rows[1]["quote"] == "USD"


def test_normalize_product_found_row_flattens_mapping_and_nutriments() -> None:
    rows = normalize(PRODUCTS, product_payload("3017620422003"), product_metadata("3017620422003"))

    assert len(rows) == 1
    assert set(rows[0]) == set(CONTRACTS[PRODUCTS].column_names)
    assert rows[0]["external_product_id"] == "3017620422003"
    assert rows[0]["internal_product_id"] is None
    assert rows[0]["mapping_status"] == "NEW"
    assert rows[0]["product_name"] == "Nutella"
    assert rows[0]["nova_group"] == 4
    assert rows[0]["energy_kcal_100g"] == 539
    assert rows[0]["salt_100g"] == 0.107
    assert rows[0]["retrieval_time"] == "2026-10-07T12:00:00+00:00"


def test_normalize_product_not_found_row_keeps_identity_and_nulls_the_payload() -> None:
    rows = normalize(
        PRODUCTS,
        product_payload("12345678", found=False),
        product_metadata("12345678", found=False),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["external_product_id"] == "12345678"
    assert row["mapping_status"] == "UNMATCHED"
    assert row["product_name"] is None
    assert row["nova_group"] is None
    assert row["salt_100g"] is None


def test_normalize_derives_mapping_status_when_sidecar_has_no_mapping() -> None:
    metadata = product_metadata("3017620422003")
    metadata.pop("mapping")
    rows = normalize(PRODUCTS, product_payload("3017620422003"), metadata)
    assert rows[0]["mapping_status"] == "NEW"
    assert rows[0]["external_product_id"] == "3017620422003"


@pytest.mark.parametrize(
    ("source", "payload", "metadata", "reason"),
    [
        (HISTORY, ["not", "an", "object"], None, "not a JSON object"),
        (
            HISTORY,
            {"daily": {"time": ["2026-09-27"], "weather_code": [3, 3]}},
            None,
            "daily.weather_code",
        ),
        (FX, [], None, "non-empty JSON array"),
        (
            FX,
            [{"date": "2026-10-06", "base": "EUR", "quote": "MAD", "rate": "x"}],
            None,
            "non-numeric rate",
        ),
        (PRODUCTS, "nope", None, "not a JSON object"),
        (
            PRODUCTS,
            {"product": {}, "code": ""},
            {"retrieved_at": "2026-10-07T00:00:00Z"},
            "external_product_id",
        ),
    ],
)
def test_normalize_rejects_structurally_invalid_payloads(
    source: str, payload: Any, metadata: dict[str, Any] | None, reason: str
) -> None:
    default_metadata = (
        product_metadata("3017620422003")
        if source == PRODUCTS
        else fx_metadata()
        if source == FX
        else weather_metadata(source, data_type="historical")
    )
    with pytest.raises(SourceFileError, match=reason):
        normalize(source, payload, metadata if metadata is not None else default_metadata)


def test_normalize_requires_matching_contract_and_valid_json() -> None:
    payload = fx_payload([fx_row()])
    metadata = fx_metadata()
    with pytest.raises(SourceFileError, match="does not match external source"):
        normalize_external(
            FX,
            json.dumps(payload).encode("utf-8"),
            json.dumps(metadata).encode("utf-8"),
            CONTRACTS[HISTORY],
        )
    with pytest.raises(SourceFileError, match="not valid JSON"):
        normalize_external(FX, b"{not json", json.dumps(metadata).encode("utf-8"), CONTRACTS[FX])


def test_external_key_helpers() -> None:
    key = (
        "raw_external/source=open_meteo_history/retrieved_date=2026-10-07/"
        "request_hash=abc/response.json"
    )
    assert is_external_key(key, "raw_external")
    assert not is_external_key(key.replace("response.json", "metadata.json"), "raw_external")
    assert not is_external_key("raw/sales/sales__0123456789abcdef.csv", "raw_external")
    assert external_source(key) == "open_meteo_history"
    assert metadata_key_for(key) == key[: -len("response.json")] + "metadata.json"


def test_batch_id_for_external_key_is_source_and_key_derived() -> None:
    key = (
        "raw_external/source=frankfurter_fx/retrieved_date=2026-10-07/"
        "request_hash=abc/response.json"
    )
    batch = batch_id_for(key, external_prefix="raw_external")
    assert batch.startswith("frankfurter_fx__")
    assert batch != "response"
    assert len(batch) == len("frankfurter_fx__") + 16
    assert batch == batch_id_for(key, external_prefix="raw_external")
    # internal objects keep their stem-based batch id
    assert batch_id_for("raw/sales/sales__0123456789abcdef.csv") == "sales__0123456789abcdef"


# --------------------------------------------------------------------------
# pipeline (in-memory store, real GX gate)
# --------------------------------------------------------------------------


def _seed_all(store: InMemoryObjectStore, settings: Settings) -> dict[str, str]:
    """Seed regions plus one valid response for every external source."""
    seed_raw(store, settings, "regions", [region_row()])
    keys = {
        "history": seed_external(
            store,
            settings,
            HISTORY,
            weather_payload(["2026-09-27"]),
            weather_metadata(HISTORY, data_type="historical"),
            digest="a" * 64,
        ),
        "forecast": seed_external(
            store,
            settings,
            FORECAST,
            weather_payload(["2026-10-07", "2026-10-08"]),
            weather_metadata(FORECAST, data_type="forecast"),
            digest="b" * 64,
        ),
        "fx": seed_external(
            store,
            settings,
            FX,
            fx_payload(
                [fx_row(), fx_row(quote="USD", rate=1.1236), fx_row(quote="GBP", rate=0.8481)]
            ),
            fx_metadata(),
            digest="c" * 64,
        ),
        "product_found": seed_external(
            store,
            settings,
            PRODUCTS,
            product_payload("3017620422003"),
            product_metadata("3017620422003"),
            digest="d" * 64,
        ),
        "product_not_found": seed_external(
            store,
            settings,
            PRODUCTS,
            product_payload("5000112637922", found=False),
            product_metadata("5000112637922", found=False),
            digest="e" * 64,
        ),
    }
    return keys


def test_external_run_promotes_every_valid_batch(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    _seed_all(store, settings)
    raw_before = _snapshot(store, RAW_BUCKET)

    result = pipeline.run(sources=list(EXTERNAL_SOURCES))

    assert len(result.records) == 5
    assert result.counts["bronzed"] == 5
    assert result.manifest["quality"] == {"passed": 5, "failed": 0}
    assert result.manifest["promoted"] == 5
    for record in result.records:
        assert record.outcome == "bronzed"
        assert record.quality_status == "passed"
        assert record.promoted is True
        assert record.expectations_failed == 0
        assert record.rows_rejected == 0
        assert record.rows_kept >= 1
        assert record.batch.startswith(f"{record.source}__")
        assert record.part_keys
        for key in record.part_keys:
            frame = pd.read_parquet(io.BytesIO(store.get_bytes(BRONZE_BUCKET, key)))
            assert PROVENANCE <= set(frame.columns)
            assert frame["source"].unique().tolist() == [record.source]
            assert len(frame) == record.rows_kept
        quality = _payload(store, record.quality_key)
        assert quality["status"] == "passed"
        assert quality["skipped_expectations"] == []
        marker = _payload(store, record.promoted_key)
        assert marker["quality"]["status"] == "passed"
    assert _snapshot(store, RAW_BUCKET) == raw_before


def test_external_metadata_object_is_never_discovered_as_a_batch(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_external(
        store,
        settings,
        HISTORY,
        weather_payload(["2026-09-27"]),
        weather_metadata(HISTORY, data_type="historical"),
        digest="a" * 64,
    )

    result = pipeline.run(sources=[HISTORY])

    assert len(result.records) == 1
    assert result.records[0].raw_key.endswith("/response.json")
    stored = set(store.list_keys(settings.bucket_raw, f"{settings.external_prefix}/"))
    assert stored == {
        result.records[0].raw_key,
        metadata_key_for(result.records[0].raw_key),
    }


def test_external_quality_failure_keeps_parts_and_report_without_promotion(
    tmp_path: Path,
) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_external(
        store, settings, FX, fx_payload([fx_row(rate=-1.0)]), fx_metadata(), digest="c" * 64
    )

    result = pipeline.run(sources=[FX])
    record = result.records[0]

    assert record.outcome == "failed"
    assert record.quality_status == "failed"
    assert record.promoted is False
    assert record.promoted_key is None
    assert record.rejected_key is None
    assert record.expectations_failed >= 1
    assert record.part_keys
    for key in record.part_keys:
        assert store.object_exists(BRONZE_BUCKET, key)
    quality = _payload(store, record.quality_key)
    assert quality["status"] == "failed"
    assert any("rate > 0" in entry["declared"] for entry in quality["failed_expectations"])
    assert store.list_keys(BRONZE_BUCKET, f"{settings.promoted_prefix}/{FX}/") == []
    assert result.counts["failed"] == 1


def test_external_conversion_rejected_row_blocks_promotion(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_external(
        store,
        settings,
        FX,
        fx_payload([fx_row(), fx_row(date=None)]),
        fx_metadata(),
        digest="c" * 64,
    )

    result = pipeline.run(sources=[FX])
    record = result.records[0]

    assert record.outcome == "failed"
    assert record.rows_in == 2
    assert record.rows_rejected == 1
    assert record.rejected_key is not None
    rejected = _payload(store, record.rejected_key)
    assert rejected["issue_counts"] == {"null_violation": 1}
    quality = _payload(store, record.quality_key)
    assert quality["status"] == "failed"
    assert "rejected" in quality["reason"]
    assert store.list_keys(BRONZE_BUCKET, f"{settings.promoted_prefix}/{FX}/") == []


def test_external_missing_metadata_sidecar_fails_without_parts(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    key = seed_external(
        store,
        settings,
        HISTORY,
        weather_payload(["2026-09-27"]),
        digest="a" * 64,
        with_metadata=False,
    )

    result = pipeline.run(sources=[HISTORY])
    record = result.records[0]

    assert record.outcome == "failed"
    assert record.quality_status == "failed"
    assert record.reason is not None and "metadata" in record.reason
    assert record.part_keys == ()
    assert store.list_keys(BRONZE_BUCKET, f"{settings.bronze_prefix}/{HISTORY}/") == []
    quality = _payload(store, record.quality_key)
    assert quality["status"] == "failed"
    assert store.list_keys(BRONZE_BUCKET, f"{settings.promoted_prefix}/") == []
    assert record.raw_key == key


def test_external_second_run_is_idempotent(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_external(store, settings, FX, fx_payload([fx_row()]), fx_metadata(), digest="c" * 64)

    first = pipeline.run(sources=[FX])
    second = pipeline.run(sources=[FX])

    assert first.records[0].outcome == "bronzed"
    assert second.records[0].outcome == "already_bronzed"
    assert second.records[0].batch == first.records[0].batch
    assert second.records[0].part_keys == first.records[0].part_keys
    assert second.records[0].promoted is True


def test_external_dry_run_plans_without_creating_the_bronze_bucket(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path, dry_run=True)
    seed_external(store, settings, FX, fx_payload([fx_row()]), fx_metadata(), digest="c" * 64)

    result = pipeline.run(sources=[FX])
    record = result.records[0]

    assert record.outcome == "planned"
    assert record.quality_status is None
    assert record.promoted_key is None
    assert record.part_keys
    assert record.rows_kept == 1
    assert not store.bucket_exists(BRONZE_BUCKET)
    assert result.manifest_path is None


def test_weather_reference_and_nullable_measures_are_promoted(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(store, settings, "regions", [region_row()])
    # temperature_2m_max missing on day one: the pair rule must not fail on nulls
    payload = weather_payload(["2026-09-27", "2026-09-28"], temperature_max=[None, 28.4])
    seed_external(
        store,
        settings,
        HISTORY,
        payload,
        weather_metadata(HISTORY, data_type="historical"),
        digest="a" * 64,
    )

    result = pipeline.run(sources=[HISTORY])
    record = result.records[0]

    assert record.outcome == "bronzed"
    assert record.quality_status == "passed"
    assert record.rows_kept == 2
    quality = _payload(store, record.quality_key)
    assert quality["status"] == "passed"
    assert quality["skipped_expectations"] == []
    frame = pd.read_parquet(io.BytesIO(store.get_bytes(BRONZE_BUCKET, record.part_keys[0])))
    assert pd.isna(frame["temperature_2m_max"].iloc[0])
    assert frame["temperature_2m_max"].iloc[1] == 28.4


def test_external_batch_is_discovered_alongside_internal_sources(tmp_path: Path) -> None:
    pipeline, settings, store = _pipeline(tmp_path)
    seed_raw(store, settings, "regions", [region_row()])
    seed_external(store, settings, FX, fx_payload([fx_row()]), fx_metadata(), digest="c" * 64)

    result = pipeline.run()

    sources = sorted(record.source for record in result.records)
    assert sources == [FX, "regions"]
    assert result.counts["bronzed"] == 2
