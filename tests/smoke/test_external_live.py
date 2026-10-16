"""Optional live smoke test for the three external APIs (network required).

Skipped by default so the normal suite never depends on the internet::

    MRI_LIVE_SMOKE=1 uv run pytest tests/smoke -q      # or: make smoke-external

One tiny, keyless request per provider; responses are held in memory only —
nothing is written to MinIO, PostgreSQL or the repository.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pytest

from mri.ingestion.clients.base import FetchOptions
from mri.ingestion.clients.fx_client import SOURCE as FX_SOURCE
from mri.ingestion.clients.fx_client import FxClient
from mri.ingestion.clients.product_catalog_client import (
    SOURCE as PRODUCTS_SOURCE,
)
from mri.ingestion.clients.product_catalog_client import (
    ProductCatalogClient,
)
from mri.ingestion.clients.weather_client import SOURCE_HISTORY, WeatherClient

pytestmark = pytest.mark.skipif(
    os.getenv("MRI_LIVE_SMOKE") != "1",
    reason="live API smoke test; set MRI_LIVE_SMOKE=1 to run it",
)

USER_AGENT = "MoroccoRetailIntelligence-Smoke/0.1.0 (noreply@example.invalid)"
PRODUCT_BARCODE = "3017620422003"


def test_live_fx_returns_mad_rate() -> None:
    http = FxClient.default_http(user_agent=USER_AGENT, timeout=20.0, max_attempts=3)
    try:
        client = FxClient(http, base_url="https://api.frankfurter.dev")
        (request,) = client.build_requests(FetchOptions(sources=(FX_SOURCE,)))
        outcome = client.fetch(request)
    finally:
        http.close()
    quotes = {row["quote"] for row in outcome.payload}
    assert "MAD" in quotes
    assert outcome.status_code == 200


def test_live_weather_history_returns_one_day() -> None:
    day = date.today() - timedelta(days=10)
    http = WeatherClient.default_http(user_agent=USER_AGENT, timeout=20.0, max_attempts=3)
    try:
        client = WeatherClient(
            http,
            archive_base_url="https://archive-api.open-meteo.com",
            forecast_base_url="https://api.open-meteo.com",
        )
        (request,) = client.build_requests(
            FetchOptions(sources=(SOURCE_HISTORY,), date=day, locations=("R01",))
        )
        outcome = client.fetch(request)
    finally:
        http.close()
    assert len(outcome.payload["daily"]["time"]) >= 1


def test_live_openfoodfacts_finds_known_product() -> None:
    http = ProductCatalogClient.default_http(
        user_agent=USER_AGENT, timeout=20.0, max_attempts=3, min_interval=4.1
    )
    try:
        client = ProductCatalogClient(http, base_url="https://world.openfoodfacts.org")
        (request,) = client.build_requests(
            FetchOptions(sources=(PRODUCTS_SOURCE,), barcodes=(PRODUCT_BARCODE,))
        )
        outcome = client.fetch(request)
    finally:
        http.close()
    assert outcome.found is True
    assert outcome.mapping is not None
    assert outcome.mapping["mapping_status"] == "NEW"
