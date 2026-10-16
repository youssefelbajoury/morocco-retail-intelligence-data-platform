"""Unit tests for the external API clients: request building, retries, pacing.

No live HTTP: every response is served by ``httpx.MockTransport`` and every
sleep goes through a fake clock. Coverage includes success, timeout, retry,
429, 500, invalid JSON, unexpected payload and empty response, plus the
no-retry-on-permanent-4xx rule.
"""

from __future__ import annotations

from datetime import date

import httpx
import pytest
from external_helpers import FakeSleep, json_response, make_http, text_response

from mri.ingestion.clients.base import FetchOptions
from mri.ingestion.clients.fx_client import SOURCE as FX_SOURCE
from mri.ingestion.clients.fx_client import FxClient
from mri.ingestion.clients.http import RateLimiter, RetryPolicy
from mri.ingestion.clients.locations import LOCATIONS, resolve_locations
from mri.ingestion.clients.product_catalog_client import (
    SOURCE as PRODUCTS_SOURCE,
)
from mri.ingestion.clients.product_catalog_client import (
    ProductCatalogClient,
    build_mapping,
    validate_barcode,
)
from mri.ingestion.clients.weather_client import (
    SOURCE_FORECAST,
    SOURCE_HISTORY,
    WeatherClient,
)
from mri.ingestion.exceptions import (
    ApiResponseError,
    ApiRetryError,
    UsageError,
)
from mri.synthetic.morocco import CITIES, REGIONS

HIST_PAYLOAD = {
    "latitude": 33.57,
    "longitude": -7.59,
    "daily": {"time": ["2024-06-01"], "temperature_2m_max": [26.4]},
}


def weather_client(handler: object, *, fake: FakeSleep | None = None, max_attempts: int = 4):
    return WeatherClient(
        make_http(handler, fake=fake, max_attempts=max_attempts),
        archive_base_url="https://archive.test",
        forecast_base_url="https://forecast.test",
    )


def fx_client(handler: object, *, fake: FakeSleep | None = None, max_attempts: int = 4):
    return FxClient(
        make_http(handler, fake=fake, max_attempts=max_attempts),
        base_url="https://fx.test",
    )


def product_client(
    handler: object,
    *,
    internal_index: dict[str, str] | None = None,
    max_attempts: int = 4,
):
    return ProductCatalogClient(
        make_http(handler, max_attempts=max_attempts),
        base_url="https://off.test",
        internal_index=internal_index,
    )


def boom(request: httpx.Request) -> httpx.Response:
    raise AssertionError(f"unexpected live request to {request.url}")


# --------------------------------------------------------------------------
# locations
# --------------------------------------------------------------------------


def test_every_location_uses_a_real_project_city() -> None:
    region_names = {region_id: name for region_id, name, _ in REGIONS}
    for location in LOCATIONS:
        assert location.city in CITIES[location.region_id]
        assert location.region_name == region_names[location.region_id]
        assert 21.0 <= location.latitude <= 36.0
        assert -18.0 <= location.longitude <= -1.0


def test_resolve_locations_accepts_ids_names_and_cities_and_dedupes() -> None:
    picked = resolve_locations(["R01", "casablanca", "Rabat"])
    assert [loc.region_id for loc in picked] == ["R01", "R02"]


def test_resolve_locations_rejects_unknown_selector() -> None:
    with pytest.raises(UsageError, match="unknown --location"):
        resolve_locations(["Atlantis"])


def test_resolve_locations_defaults_to_all_regions() -> None:
    assert len(resolve_locations([])) == 12


# --------------------------------------------------------------------------
# weather request building
# --------------------------------------------------------------------------


def test_weather_history_request_shape() -> None:
    client = weather_client(boom)
    options = FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    (request,) = client.build_requests(options)
    assert request.source == SOURCE_HISTORY
    assert request.data_type == "historical"
    assert request.url == "https://archive.test/v1/archive"
    assert request.params["start_date"] == "2024-06-01"
    assert request.params["end_date"] == "2024-06-01"
    assert request.params["timezone"] == "Africa/Casablanca"
    assert "weather_code" in request.params["daily"]
    assert request.params["latitude"].startswith("33.57")
    assert request.params["longitude"].startswith("-7.58")
    assert request.context["api_key_required"] is False
    assert request.context["location"]["city"] == "Casablanca"


def test_weather_forecast_request_shape_and_horizon() -> None:
    client = weather_client(boom)
    options = FetchOptions(sources=(SOURCE_FORECAST,), forecast_days=3, locations=("R02",))
    (request,) = client.build_requests(options)
    assert request.url == "https://forecast.test/v1/forecast"
    assert request.data_type == "forecast"
    assert request.params["forecast_days"] == "3"
    assert "start_date" not in request.params


def test_weather_covers_all_regions_by_default_and_honours_limit() -> None:
    client = weather_client(boom)
    everything = client.build_requests(FetchOptions(sources=(SOURCE_HISTORY,)))
    assert len(everything) == 12
    limited = client.build_requests(FetchOptions(sources=(SOURCE_HISTORY,), limit=2))
    assert len(limited) == 2


def test_weather_rejects_bad_forecast_horizon() -> None:
    client = weather_client(boom)
    with pytest.raises(UsageError, match="forecast-days"):
        client.build_requests(FetchOptions(sources=(SOURCE_FORECAST,), forecast_days=99))


def test_weather_rejects_inverted_date_range() -> None:
    client = weather_client(boom)
    with pytest.raises(UsageError, match="after end date"):
        client.build_requests(
            FetchOptions(
                sources=(SOURCE_HISTORY,),
                start_date=date(2024, 6, 10),
                end_date=date(2024, 6, 1),
            )
        )


# --------------------------------------------------------------------------
# HTTP behaviours: success, timeout, retry, 429, 500, invalid JSON
# --------------------------------------------------------------------------


def test_weather_fetch_success_returns_verbatim_bytes() -> None:
    client = weather_client(lambda request: json_response(request, HIST_PAYLOAD))
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    outcome = client.fetch(request)
    assert outcome.status_code == 200
    assert outcome.attempts == 1
    assert outcome.payload == HIST_PAYLOAD
    assert outcome.raw.decode("utf-8").startswith("{")


def test_invalid_json_is_permanent_and_not_retried() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return text_response(request, "<html>oops</html>")

    client = weather_client(handler)
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    with pytest.raises(ApiResponseError) as excinfo:
        client.fetch(request)
    assert excinfo.value.reason == "invalid_json"
    assert len(calls) == 1


def test_unexpected_payload_is_detected() -> None:
    client = weather_client(lambda request: json_response(request, {"nope": True}))
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    with pytest.raises(ApiResponseError) as excinfo:
        client.fetch(request)
    assert excinfo.value.reason == "unexpected_payload"


def test_empty_daily_series_is_detected() -> None:
    payload = {"latitude": 33.5, "daily": {"time": []}}
    client = weather_client(lambda request: json_response(request, payload))
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    with pytest.raises(ApiResponseError) as excinfo:
        client.fetch(request)
    assert excinfo.value.reason == "empty_response"


def test_timeout_is_retried_then_succeeds() -> None:
    attempts: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        attempts.append(1)
        if len(attempts) < 3:
            raise httpx.ConnectTimeout("connection timed out", request=request)
        return json_response(request, HIST_PAYLOAD)

    fake = FakeSleep()
    client = weather_client(handler, fake=fake)
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    outcome = client.fetch(request)
    assert outcome.attempts == 3
    assert len(fake.slept) == 2
    assert all(delay > 0 for delay in fake.slept)


def test_timeout_exhausts_bounded_retries() -> None:
    fake = FakeSleep()

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    client = weather_client(handler, fake=fake, max_attempts=3)
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    with pytest.raises(ApiRetryError) as excinfo:
        client.fetch(request)
    assert excinfo.value.attempts == 3
    assert excinfo.value.reason == "timeout"
    assert len(fake.slept) == 2


def test_429_retries_and_honours_retry_after() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(
                429,
                headers={"Retry-After": "7"},
                text="slow down",
                request=request,
            )
        return json_response(request, HIST_PAYLOAD)

    fake = FakeSleep()
    client = weather_client(handler, fake=fake)
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    outcome = client.fetch(request)
    assert outcome.attempts == 2
    assert fake.slept[0] >= 7.0


def test_500_exhausts_retries_and_reports_status() -> None:
    fake = FakeSleep()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom", request=request)

    client = weather_client(handler, fake=fake, max_attempts=3)
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    with pytest.raises(ApiRetryError) as excinfo:
        client.fetch(request)
    assert excinfo.value.status_code == 500
    assert excinfo.value.reason == "http_500"
    assert excinfo.value.attempts == 3
    assert len(fake.slept) == 2


def test_cloudflare_origin_errors_are_retried() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        if len(calls) == 1:
            return httpx.Response(522, text="connection timed out", request=request)
        return json_response(request, HIST_PAYLOAD)

    client = weather_client(handler, fake=FakeSleep())
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    outcome = client.fetch(request)
    assert outcome.attempts == 2
    assert len(calls) == 2


def test_permanent_400_is_never_retried() -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(400, text="bad request", request=request)

    client = weather_client(handler)
    (request,) = client.build_requests(
        FetchOptions(sources=(SOURCE_HISTORY,), date=date(2024, 6, 1), locations=("R01",))
    )
    with pytest.raises(ApiResponseError) as excinfo:
        client.fetch(request)
    assert excinfo.value.reason == "http_400"
    assert excinfo.value.status_code == 400
    assert len(calls) == 1


# --------------------------------------------------------------------------
# frankfurter
# --------------------------------------------------------------------------


def test_fx_request_contains_verified_mad_pairs() -> None:
    client = fx_client(boom)
    (request,) = client.build_requests(FetchOptions(sources=(FX_SOURCE,), date=date(2024, 6, 3)))
    assert request.url == "https://fx.test/v2/rates"
    assert request.params["base"] == "EUR"
    assert request.params["quotes"] == "MAD,USD,GBP"
    assert request.params["date"] == "2024-06-03"
    assert request.context["api_key_required"] is False


def test_fx_range_and_single_date_are_validated() -> None:
    client = fx_client(boom)
    with pytest.raises(UsageError, match="--date cannot be combined"):
        client.build_requests(
            FetchOptions(
                sources=(FX_SOURCE,),
                date=date(2024, 6, 3),
                start_date=date(2024, 6, 1),
                end_date=date(2024, 6, 2),
            )
        )
    with pytest.raises(UsageError, match="must be given together"):
        client.build_requests(FetchOptions(sources=(FX_SOURCE,), start_date=date(2024, 6, 1)))


def test_fx_builds_range_request() -> None:
    client = fx_client(boom)
    (request,) = client.build_requests(
        FetchOptions(
            sources=(FX_SOURCE,),
            start_date=date(2021, 1, 1),
            end_date=date(2021, 1, 31),
        )
    )
    assert request.params["start_date"] == "2021-01-01"
    assert request.params["end_date"] == "2021-01-31"
    assert "date" not in request.params


def test_fx_fetch_success_validates_records() -> None:
    rows = [{"date": "2024-06-03", "base": "EUR", "quote": "MAD", "rate": 10.7}]
    client = fx_client(lambda request: json_response(request, rows))
    (request,) = client.build_requests(FetchOptions(sources=(FX_SOURCE,), date=date(2024, 6, 3)))
    outcome = client.fetch(request)
    assert outcome.payload == rows
    assert outcome.attempts == 1


def test_fx_empty_response_is_rejected() -> None:
    client = fx_client(lambda request: json_response(request, []))
    (request,) = client.build_requests(FetchOptions(sources=(FX_SOURCE,)))
    with pytest.raises(ApiResponseError) as excinfo:
        client.fetch(request)
    assert excinfo.value.reason == "empty_response"


def test_fx_unexpected_payloads_are_rejected() -> None:
    shapes = [
        {"date": "2024-06-03"},
        [{"date": "2024-06-03", "base": "EUR", "quote": "MAD"}],
        [{"date": "2024-06-03", "base": "EUR", "quote": "MAD", "rate": "high"}],
        [{"date": "2024-06-03", "base": "EUR", "quote": "MAD", "rate": True}],
    ]
    for payload in shapes:
        client = fx_client(lambda request, p=payload: json_response(request, p))
        (request,) = client.build_requests(FetchOptions(sources=(FX_SOURCE,)))
        with pytest.raises(ApiResponseError) as excinfo:
            client.fetch(request)
        assert excinfo.value.reason == "unexpected_payload", payload


# --------------------------------------------------------------------------
# open food facts
# --------------------------------------------------------------------------


def test_barcode_validation_accepts_gtin_and_rejects_garbage() -> None:
    assert validate_barcode(" 3017620422003 ") == "3017620422003"
    for bad in ("123", "abc", "3017620422003x", ""):
        with pytest.raises(UsageError, match="invalid barcode"):
            validate_barcode(bad)


def test_products_source_requires_barcodes() -> None:
    client = product_client(boom)
    with pytest.raises(UsageError, match="at least one barcode"):
        client.build_requests(FetchOptions(sources=(PRODUCTS_SOURCE,)))


def test_product_request_url_and_limit() -> None:
    client = product_client(boom)
    options = FetchOptions(
        sources=(PRODUCTS_SOURCE,),
        barcodes=("3017620422003", "737628064502", "5000112637922"),
        limit=2,
    )
    requests = client.build_requests(options)
    assert len(requests) == 2
    assert requests[0].url == "https://off.test/api/v3/product/3017620422003.json"
    assert requests[0].context["api_key_required"] is False


def test_product_fetch_found_maps_to_new_without_internal_index() -> None:
    payload = {"code": "3017620422003", "errors": [], "product": {"product_name": "Nutella"}}
    client = product_client(lambda request: json_response(request, payload))
    (request,) = client.build_requests(
        FetchOptions(sources=(PRODUCTS_SOURCE,), barcodes=("3017620422003",))
    )
    outcome = client.fetch(request)
    assert outcome.found is True
    assert outcome.mapping == {
        "external_product_id": "3017620422003",
        "internal_product_id": None,
        "source": PRODUCTS_SOURCE,
        "mapping_status": "NEW",
    }


def test_product_fetch_matched_requires_exact_internal_barcode() -> None:
    payload = {"code": "3017620422003", "product": {"product_name": "Nutella"}}
    client = product_client(
        lambda request: json_response(request, payload),
        internal_index={"3017620422003": "P000042"},
    )
    (request,) = client.build_requests(
        FetchOptions(sources=(PRODUCTS_SOURCE,), barcodes=("3017620422003",))
    )
    outcome = client.fetch(request)
    assert outcome.mapping is not None
    assert outcome.mapping["mapping_status"] == "MATCHED"
    assert outcome.mapping["internal_product_id"] == "P000042"


def test_product_fetch_not_found_maps_to_unmatched() -> None:
    payload = {"code": "9999999999999", "errors": ["product not found"], "status": 0}
    client = product_client(
        lambda request: json_response(request, payload),
        internal_index={"9999999999999": "P000007"},
    )
    (request,) = client.build_requests(
        FetchOptions(sources=(PRODUCTS_SOURCE,), barcodes=("9999999999999",))
    )
    outcome = client.fetch(request)
    assert outcome.found is False
    assert outcome.mapping is not None
    assert outcome.mapping["mapping_status"] == "UNMATCHED"
    assert outcome.mapping["internal_product_id"] == "P000007"


def test_product_fetch_rejects_unexpected_payload() -> None:
    client = product_client(lambda request: json_response(request, {"hello": "world"}))
    (request,) = client.build_requests(
        FetchOptions(sources=(PRODUCTS_SOURCE,), barcodes=("3017620422003",))
    )
    with pytest.raises(ApiResponseError) as excinfo:
        client.fetch(request)
    assert excinfo.value.reason == "unexpected_payload"


def test_mapping_never_matches_without_both_sides() -> None:
    assert build_mapping("1", internal_index={}, found=True)["mapping_status"] == "NEW"
    assert (
        build_mapping("1", internal_index={"1": "P1"}, found=False)["mapping_status"] == "UNMATCHED"
    )
    assert build_mapping("1", internal_index={"1": "P1"}, found=True)["mapping_status"] == "MATCHED"


# --------------------------------------------------------------------------
# shared transport primitives
# --------------------------------------------------------------------------


def test_rate_limiter_enforces_minimum_interval() -> None:
    fake = FakeSleep()
    limiter = RateLimiter(4.1, sleep=fake.sleep, clock=fake.clock)
    assert limiter.wait() == 0.0
    assert fake.slept == []
    fake.now = 1.0
    assert limiter.wait() == pytest.approx(3.1)
    assert fake.slept == [pytest.approx(3.1)]


def test_rate_limiter_zero_interval_never_sleeps() -> None:
    fake = FakeSleep()
    limiter = RateLimiter(0.0, sleep=fake.sleep, clock=fake.clock)
    limiter.wait()
    limiter.wait()
    assert fake.slept == []


def test_backoff_grows_exponentially_and_caps() -> None:
    policy = RetryPolicy(backoff_base=1.0, backoff_factor=2.0, backoff_cap=3.0, jitter_ratio=0.0)
    import random

    rng = random.Random(0)
    assert [policy.backoff_for(n, rng) for n in (1, 2, 3, 4)] == [1.0, 2.0, 3.0, 3.0]


def test_retry_after_parses_seconds_http_date_and_caps() -> None:
    policy = RetryPolicy(retry_after_cap=60.0)
    assert policy.retry_after_for("3") == 3.0
    assert policy.retry_after_for("3600") == 60.0
    assert policy.retry_after_for("Wed, 21 Oct 2015 07:28:00 GMT") == 0.0
    assert policy.retry_after_for("not-a-date") is None
    assert policy.retry_after_for(None) is None
