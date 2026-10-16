"""Open-Meteo client: observed/historical weather and forecast for Morocco.

Two distinct datasets with explicit provenance (``data_type``):

* ``open_meteo_history`` — ``/v1/archive`` reanalysis (ERA5/IFS), observed data
  for an explicit ``start_date``/``end_date`` window;
* ``open_meteo_forecast`` — ``/v1/forecast`` forward-looking days only.

No API key is required for non-commercial use (documented by Open-Meteo);
a key would only be needed for commercial reservations. Locations come from the
project's region table (:mod:`mri.ingestion.clients.locations`), never from
invented coordinates. Daily aggregates are requested in the project's own
timezone (``Africa/Casablanca``), which the API requires alongside ``daily``.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from mri.ingestion.clients.base import FetchOptions, FetchOutcome, FetchRequest, selected
from mri.ingestion.clients.http import HttpClient, RateLimiter, RetryPolicy
from mri.ingestion.clients.locations import Location, resolve_locations
from mri.ingestion.exceptions import ApiResponseError, UsageError

SOURCE_HISTORY = "open_meteo_history"
SOURCE_FORECAST = "open_meteo_forecast"
SOURCES: tuple[str, ...] = (SOURCE_HISTORY, SOURCE_FORECAST)

PROVIDER = "Open-Meteo"
API_DOCS = "https://open-meteo.com/en/docs"
TERMS = "https://open-meteo.com/en/licence"
PROJECT_TIMEZONE = "Africa/Casablanca"
MAX_FORECAST_DAYS = 16
DEFAULT_DAILY_VARIABLES = (
    "weather_code",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "precipitation_hours",
    "wind_speed_10m_max",
)
DEFAULT_MIN_INTERVAL_SECONDS = 1.0


def _common_context(location: Location, dataset: str) -> dict[str, Any]:
    return {
        "provider": PROVIDER,
        "api_docs": API_DOCS,
        "terms": TERMS,
        "license": "CC BY 4.0 (non-commercial free tier)",
        "api_key_required": False,
        "data_type": dataset,
        "location": location.as_dict(),
        "timezone": PROJECT_TIMEZONE,
        "min_interval_seconds": DEFAULT_MIN_INTERVAL_SECONDS,
    }


class WeatherClient:
    """Builds and executes Open-Meteo daily-weather requests."""

    sources = SOURCES

    def __init__(
        self,
        http: HttpClient,
        *,
        archive_base_url: str,
        forecast_base_url: str,
        daily_variables: tuple[str, ...] = DEFAULT_DAILY_VARIABLES,
    ) -> None:
        self._http = http
        self._archive_base = archive_base_url.rstrip("/")
        self._forecast_base = forecast_base_url.rstrip("/")
        self._daily_variables = daily_variables

    @staticmethod
    def default_http(
        *,
        user_agent: str,
        timeout: float,
        max_attempts: int,
    ) -> HttpClient:
        return HttpClient(
            user_agent=user_agent,
            timeout=timeout,
            retry=RetryPolicy(max_attempts=max_attempts),
            rate_limiter=RateLimiter(DEFAULT_MIN_INTERVAL_SECONDS),
        )

    def build_requests(self, options: FetchOptions) -> list[FetchRequest]:
        locations = resolve_locations(options.locations)
        requests: list[FetchRequest] = []

        if SOURCE_HISTORY in selected(options, self.sources):
            start = options.start_date or options.date or (date.today() - timedelta(days=1))
            end = options.end_date or options.date or start
            if start > end:
                raise UsageError(f"weather start date {start} is after end date {end}")
            for location in locations:
                requests.append(
                    FetchRequest(
                        source=SOURCE_HISTORY,
                        data_type="historical",
                        url=f"{self._archive_base}/v1/archive",
                        params={
                            "latitude": f"{location.latitude:.4f}",
                            "longitude": f"{location.longitude:.4f}",
                            "start_date": start.isoformat(),
                            "end_date": end.isoformat(),
                            "daily": ",".join(self._daily_variables),
                            "timezone": PROJECT_TIMEZONE,
                            "timeformat": "iso8601",
                        },
                        context=_common_context(location, "historical"),
                    )
                )

        if SOURCE_FORECAST in selected(options, self.sources):
            days = options.forecast_days
            if not 1 <= days <= MAX_FORECAST_DAYS:
                raise UsageError(
                    f"--forecast-days must be between 1 and {MAX_FORECAST_DAYS}, got {days}"
                )
            for location in locations:
                requests.append(
                    FetchRequest(
                        source=SOURCE_FORECAST,
                        data_type="forecast",
                        url=f"{self._forecast_base}/v1/forecast",
                        params={
                            "latitude": f"{location.latitude:.4f}",
                            "longitude": f"{location.longitude:.4f}",
                            "daily": ",".join(self._daily_variables),
                            "forecast_days": str(days),
                            "timezone": PROJECT_TIMEZONE,
                            "timeformat": "iso8601",
                        },
                        context=_common_context(location, "forecast"),
                    )
                )

        if options.limit is not None:
            requests = requests[: options.limit]
        return requests

    def fetch(self, request: FetchRequest) -> FetchOutcome:
        result = self._http.get_json(request.url, request.params)
        payload = result.payload
        if not isinstance(payload, dict) or "latitude" not in payload:
            raise ApiResponseError(
                f"unexpected Open-Meteo payload from {result.url}",
                reason="unexpected_payload",
                status_code=result.status_code,
            )
        daily = payload.get("daily")
        if not isinstance(daily, dict) or not daily.get("time"):
            raise ApiResponseError(
                f"empty daily series from {result.url}",
                reason="empty_response",
                status_code=result.status_code,
            )
        return FetchOutcome(
            request=request,
            payload=payload,
            raw=result.text.encode("utf-8"),
            status_code=result.status_code,
            attempts=result.attempts,
            elapsed_ms=result.elapsed_ms,
        )
