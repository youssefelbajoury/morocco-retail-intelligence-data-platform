"""Normalization of stored external API responses into contract-shaped rows.

Raw external objects (``raw_external/.../{response,metadata}.json``) are
verbatim API payloads: nested objects, parallel daily arrays and provider
field names.  Bronze needs flat, contract-shaped rows, so this module is the
one place that knows how to reshape each of the four external payloads:

* ``open_meteo_history`` / ``open_meteo_forecast`` — expand the parallel
  ``daily`` arrays into one row per day, stamped with the project region from
  the metadata sidecar (``context.location``);
* ``frankfurter_fx`` — pass the rate records through, keeping only the four
  contract columns;
* ``openfoodfacts_products`` — flatten one product (plus its mapping sidecar
  and nutriments) into one row per fetched barcode.

Normalization is pure and side-effect free: bytes in, bytes out.  Anything
structurally wrong (payload is not the expected shape, metadata sidecar
missing or malformed, daily arrays of unequal length) raises
:class:`~mri.ingestion.exceptions.SourceFileError`, which Bronze reports as a
failed batch with a quality report and no promotion marker.  Cell-level
problems are *not* handled here — they are left to the strict standardizer
and the Great Expectations gate.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from mri.ingestion.clients.weather_client import DEFAULT_DAILY_VARIABLES
from mri.ingestion.contracts import Contract
from mri.ingestion.exceptions import SourceFileError

SOURCE_HISTORY = "open_meteo_history"
SOURCE_FORECAST = "open_meteo_forecast"
SOURCE_FX = "frankfurter_fx"
SOURCE_PRODUCTS = "openfoodfacts_products"

RESPONSE_SUFFIX = "response.json"
METADATA_SUFFIX = "metadata.json"

MAPPING_STATUSES = ("MATCHED", "NEW", "UNMATCHED")

Row = dict[str, Any]
Normalizer = Callable[[Any, Any], list[Row]]


def is_external_key(key: str, prefix: str) -> bool:
    """True for a stored external response object (``<prefix>/.../response.json``)."""
    base = prefix.strip("/")
    return key.startswith(f"{base}/") and key.endswith(f"/{RESPONSE_SUFFIX}")


def external_source(key: str) -> str:
    """Extract the ``source=`` segment from an external object key (``""`` if absent)."""
    for part in key.split("/"):
        if part.startswith("source="):
            return part[len("source=") :]
    return ""


def metadata_key_for(response_key: str) -> str:
    """The provenance sidecar that lives next to one response object."""
    return response_key[: -len(RESPONSE_SUFFIX)] + METADATA_SUFFIX


def normalize_external(
    source: str,
    payload_bytes: bytes,
    metadata_bytes: bytes,
    contract: Contract,
) -> bytes:
    """Flatten one stored response + metadata pair into a JSON array of rows.

    The returned bytes are handed straight to
    :func:`mri.ingestion.bronze_schema.parse_rows` (``fmt="json"``), so every
    emitted key must be a declared contract column and every value must be a
    scalar (string, number, bool or ``null``).
    """
    if source != contract.id:
        raise SourceFileError(f"contract {contract.id} does not match external source {source}")
    normalizer = _NORMALIZERS.get(source)
    if normalizer is None:
        raise SourceFileError(f"no external normalizer for source {source!r}")
    payload = _load_json(payload_bytes, source, "response")
    metadata = _load_json(metadata_bytes, source, "metadata")

    rows = normalizer(payload, metadata)
    if not rows:
        raise SourceFileError(f"{source}: normalized payload produced no rows")
    known = set(contract.column_names)
    for position, row in enumerate(rows):
        unknown = sorted(set(row) - known)
        if unknown:
            raise SourceFileError(
                f"{source}: normalized row {position} has columns outside the "
                f"contract: {', '.join(unknown)}"
            )
    return json.dumps(rows, ensure_ascii=False).encode("utf-8")


def _load_json(data: bytes, source: str, kind: str) -> Any:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SourceFileError(f"{source}: {kind} object is not valid UTF-8: {exc}") from exc
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise SourceFileError(f"{source}: {kind} object is not valid JSON: {exc}") from exc


def _require_str(value: Any, where: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SourceFileError(f"{where} is missing or not a string")
    return value


def _weather_rows(payload: Any, metadata: Any, *, date_column: str) -> list[Row]:
    if not isinstance(payload, dict):
        raise SourceFileError("weather payload is not a JSON object")
    daily = payload.get("daily")
    if not isinstance(daily, dict):
        raise SourceFileError("weather payload has no daily object")
    times = daily.get("time")
    if not isinstance(times, list) or not times:
        raise SourceFileError("weather payload has no daily time series")
    context = metadata.get("context") if isinstance(metadata, dict) else None
    if not isinstance(context, dict):
        raise SourceFileError("weather metadata has no context object")
    location = context.get("location")
    if not isinstance(location, dict):
        raise SourceFileError("weather metadata context has no location object")

    region_id = _require_str(location.get("region_id"), "metadata context.location.region_id")
    region_name = _require_str(location.get("region_name"), "metadata context.location.region_name")
    city = _require_str(location.get("city"), "metadata context.location.city")
    retrieved_at = _require_str(metadata.get("retrieved_at"), "metadata.retrieved_at")
    data_type = _require_str(metadata.get("data_type"), "metadata.data_type")
    timezone = context.get("timezone") or payload.get("timezone")
    timezone = _require_str(timezone, "metadata context.timezone")
    latitude = payload.get("latitude")
    if latitude is None:
        latitude = location.get("latitude")
    longitude = payload.get("longitude")
    if longitude is None:
        longitude = location.get("longitude")
    if latitude is None or longitude is None:
        raise SourceFileError("weather payload and metadata carry no coordinates")

    series: dict[str, list[Any]] = {}
    for name in DEFAULT_DAILY_VARIABLES:
        values = daily.get(name)
        if not isinstance(values, list) or len(values) != len(times):
            found = "missing" if not isinstance(values, list) else f"length {len(values)}"
            raise SourceFileError(
                f"weather payload daily.{name} is {found}; expected {len(times)} values"
            )
        series[name] = values

    rows: list[Row] = []
    for position, day in enumerate(times):
        if not isinstance(day, str):
            raise SourceFileError(
                f"weather payload daily.time[{position}] is not an ISO-8601 date string"
            )
        row: Row = {
            "region_id": region_id,
            "region_name": region_name,
            "city": city,
            "timezone": timezone,
            "latitude": latitude,
            "longitude": longitude,
            date_column: day,
            "data_type": data_type,
            "retrieval_time": retrieved_at,
        }
        row.update({name: series[name][position] for name in DEFAULT_DAILY_VARIABLES})
        rows.append(row)
    return rows


def _history_rows(payload: Any, metadata: Any) -> list[Row]:
    return _weather_rows(payload, metadata, date_column="observation_date")


def _forecast_rows(payload: Any, metadata: Any) -> list[Row]:
    return _weather_rows(payload, metadata, date_column="forecast_date")


def _fx_rows(payload: Any, metadata: Any) -> list[Row]:
    if not isinstance(payload, list) or not payload:
        raise SourceFileError("fx payload is not a non-empty JSON array")
    rows: list[Row] = []
    for position, item in enumerate(payload):
        if not isinstance(item, dict):
            raise SourceFileError(f"fx rate record {position} is not a JSON object")
        rate = item.get("rate")
        if isinstance(rate, bool) or not isinstance(rate, (int, float)):
            raise SourceFileError(f"fx rate record {position} has a non-numeric rate")
        rows.append(
            {
                "date": item.get("date"),
                "base": item.get("base"),
                "quote": item.get("quote"),
                "rate": rate,
            }
        )
    return rows


def _product_rows(payload: Any, metadata: Any) -> list[Row]:
    if not isinstance(payload, dict):
        raise SourceFileError("product payload is not a JSON object")
    product = payload.get("product") or {}
    if not isinstance(product, dict):
        raise SourceFileError("product payload product field is not a JSON object")
    mapping = metadata.get("mapping")
    if mapping is None:
        mapping = {}
    if not isinstance(mapping, dict):
        raise SourceFileError("product metadata mapping is not a JSON object")

    found = metadata.get("result") == "found" or bool(product)
    external_id = (
        _optional_str(mapping.get("external_product_id"))
        or _optional_str(payload.get("code"))
        or _optional_str(product.get("code"))
    )
    if external_id is None:
        raise SourceFileError("cannot determine external_product_id from payload or metadata")
    status = _optional_str(mapping.get("mapping_status")) or ("NEW" if found else "UNMATCHED")
    if status not in MAPPING_STATUSES:
        raise SourceFileError(f"metadata mapping.mapping_status {status!r} is not a known status")
    retrieved_at = _require_str(metadata.get("retrieved_at"), "metadata.retrieved_at")

    nutriments = product.get("nutriments")
    if not isinstance(nutriments, dict):
        nutriments = {}
    return [
        {
            "external_product_id": external_id,
            "internal_product_id": mapping.get("internal_product_id"),
            "mapping_status": status,
            "retrieval_time": retrieved_at,
            "product_name": product.get("product_name"),
            "brands": product.get("brands"),
            "quantity": product.get("quantity"),
            "categories": product.get("categories"),
            "nutriscore_grade": product.get("nutriscore_grade"),
            "nova_group": product.get("nova_group"),
            "image_url": product.get("image_url"),
            "countries": product.get("countries"),
            "lang": product.get("lang"),
            "ingredients_text": product.get("ingredients_text"),
            "allergens": product.get("allergens"),
            "product_quantity": product.get("product_quantity"),
            "energy_kcal_100g": nutriments.get("energy-kcal_100g"),
            "fat_100g": nutriments.get("fat_100g"),
            "sugars_100g": nutriments.get("sugars_100g"),
            "proteins_100g": nutriments.get("proteins_100g"),
            "salt_100g": nutriments.get("salt_100g"),
        }
    ]


def _optional_str(value: Any) -> str | None:
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


_NORMALIZERS: dict[str, Normalizer] = {
    SOURCE_HISTORY: _history_rows,
    SOURCE_FORECAST: _forecast_rows,
    SOURCE_FX: _fx_rows,
    SOURCE_PRODUCTS: _product_rows,
}

__all__ = [
    "MAPPING_STATUSES",
    "METADATA_SUFFIX",
    "RESPONSE_SUFFIX",
    "external_source",
    "is_external_key",
    "metadata_key_for",
    "normalize_external",
]
