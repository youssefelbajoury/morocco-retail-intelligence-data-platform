"""Frankfurter client: ECB reference exchange rates (no API key).

Verified against the official v2 API (``https://api.frankfurter.dev``): the
currency list contains ``MAD`` (Moroccan Dirham), so the project pairs
EUR/MAD, USD/MAD and GBP/MAD are available. One request covers either the
latest rates (no date), a single ``date`` or a ``start_date``/``end_date``
range, which returns one record per (date, base, quote).
"""

from __future__ import annotations

from typing import Any

from mri.ingestion.clients.base import FetchOptions, FetchOutcome, FetchRequest, selected
from mri.ingestion.clients.http import HttpClient, RateLimiter, RetryPolicy
from mri.ingestion.exceptions import ApiResponseError, UsageError

SOURCE = "frankfurter_fx"
SOURCES: tuple[str, ...] = (SOURCE,)

PROVIDER = "Frankfurter (ECB reference rates)"
API_DOCS = "https://frankfurter.dev"
TERMS = "https://frankfurter.dev/"
DATA_TYPE = "fx_rates"

BASE_CURRENCY = "EUR"
# Verified present in https://api.frankfurter.dev/v2/currencies (MAD = 504).
QUOTE_CURRENCIES = ("MAD", "USD", "GBP")
DEFAULT_MIN_INTERVAL_SECONDS = 1.0


class FxClient:
    """Builds and executes Frankfurter rate requests."""

    sources = SOURCES

    def __init__(self, http: HttpClient, *, base_url: str) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")

    @staticmethod
    def default_http(*, user_agent: str, timeout: float, max_attempts: int) -> HttpClient:
        return HttpClient(
            user_agent=user_agent,
            timeout=timeout,
            retry=RetryPolicy(max_attempts=max_attempts),
            rate_limiter=RateLimiter(DEFAULT_MIN_INTERVAL_SECONDS),
        )

    def build_requests(self, options: FetchOptions) -> list[FetchRequest]:
        if not selected(options, self.sources):
            return []
        if options.date and (options.start_date or options.end_date):
            raise UsageError("--date cannot be combined with --start-date/--end-date")
        if bool(options.start_date) != bool(options.end_date):
            raise UsageError("--start-date and --end-date must be given together")

        params: dict[str, str] = {
            "base": BASE_CURRENCY,
            "quotes": ",".join(QUOTE_CURRENCIES),
        }
        if options.date:
            params["date"] = options.date.isoformat()
        elif options.start_date and options.end_date:
            if options.start_date > options.end_date:
                raise UsageError(
                    f"fx start date {options.start_date} is after end date {options.end_date}"
                )
            params["start_date"] = options.start_date.isoformat()
            params["end_date"] = options.end_date.isoformat()

        context: dict[str, Any] = {
            "provider": PROVIDER,
            "api_docs": API_DOCS,
            "terms": TERMS,
            "license": "ECB reference rates, free for any use (no key required)",
            "api_key_required": False,
            "data_type": DATA_TYPE,
            "base": BASE_CURRENCY,
            "quotes": list(QUOTE_CURRENCIES),
            "min_interval_seconds": DEFAULT_MIN_INTERVAL_SECONDS,
        }
        return [
            FetchRequest(
                source=SOURCE,
                data_type=DATA_TYPE,
                url=f"{self._base_url}/v2/rates",
                params=params,
                context=context,
            )
        ]

    def fetch(self, request: FetchRequest) -> FetchOutcome:
        result = self._http.get_json(request.url, request.params)
        payload = result.payload
        if not isinstance(payload, list):
            raise ApiResponseError(
                f"unexpected Frankfurter payload (expected a list) from {result.url}",
                reason="unexpected_payload",
                status_code=result.status_code,
            )
        if not payload:
            raise ApiResponseError(
                f"empty rate series from {result.url}",
                reason="empty_response",
                status_code=result.status_code,
            )
        for row in payload:
            if not isinstance(row, dict) or not {"date", "base", "quote", "rate"} <= set(row):
                raise ApiResponseError(
                    f"unexpected Frankfurter rate record from {result.url}",
                    reason="unexpected_payload",
                    status_code=result.status_code,
                )
            rate = row.get("rate")
            if isinstance(rate, bool) or not isinstance(rate, (int, float)):
                raise ApiResponseError(
                    f"non-numeric rate in Frankfurter response from {result.url}",
                    reason="unexpected_payload",
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
