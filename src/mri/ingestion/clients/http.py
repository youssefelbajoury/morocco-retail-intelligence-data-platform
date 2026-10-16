"""The single HTTP utility shared by every external API client.

One library (``httpx``), one retry policy, one pacing primitive:

* bounded retries with exponential backoff and jitter for timeouts, connection
  errors, ``429`` and ``5xx`` responses;
* ``Retry-After`` (seconds or HTTP-date) honoured and capped;
* every other ``4xx`` fails immediately and is **never** retried;
* an optional :class:`RateLimiter` enforces a minimum interval between
  requests so documented per-IP limits are respected;
* responses are parsed strictly as JSON — malformed bodies raise a permanent
  :class:`~mri.ingestion.exceptions.ApiResponseError`.

Sleeping and clock are injectable so tests never wait in real time.
"""

from __future__ import annotations

import json
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from mri.ingestion.exceptions import ApiResponseError, ApiRetryError

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504, 520, 521, 522, 523, 524})


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded retry configuration for transient failures."""

    max_attempts: int = 4
    backoff_base: float = 0.5
    backoff_factor: float = 2.0
    backoff_cap: float = 30.0
    jitter_ratio: float = 0.25
    retry_after_cap: float = 60.0

    def backoff_for(self, attempt: int, rng: random.Random) -> float:
        """Seconds to sleep after a failed ``attempt`` (1-based), jittered."""
        raw = min(self.backoff_cap, self.backoff_base * (self.backoff_factor ** (attempt - 1)))
        if self.jitter_ratio:
            raw *= 1.0 + rng.uniform(-self.jitter_ratio, self.jitter_ratio)
        return max(0.0, raw)

    def retry_after_for(self, header_value: str | None) -> float | None:
        """Parse a ``Retry-After`` header (seconds or HTTP-date) into seconds."""
        if not header_value:
            return None
        value = header_value.strip()
        try:
            seconds = float(value)
        except ValueError:
            try:
                target = parsedate_to_datetime(value)
            except (TypeError, ValueError):
                return None
            if target.tzinfo is None:
                target = target.replace(tzinfo=UTC)
            seconds = (target - datetime.now(UTC)).total_seconds()
        if seconds < 0:
            return 0.0
        return min(seconds, self.retry_after_cap)


class RateLimiter:
    """Enforce a minimum wall-clock interval between consecutive requests."""

    def __init__(
        self,
        min_interval: float,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if min_interval < 0:
            raise ValueError("min_interval must be >= 0")
        self._min_interval = min_interval
        self._sleep = sleep
        self._clock = clock
        self._last: float | None = None

    @property
    def min_interval(self) -> float:
        return self._min_interval

    def wait(self) -> float:
        """Sleep if needed; return the number of seconds actually slept."""
        slept = 0.0
        if self._last is not None:
            remaining = self._min_interval - (self._clock() - self._last)
            if remaining > 0:
                self._sleep(remaining)
                slept = remaining
        self._last = self._clock()
        return slept


@dataclass(frozen=True)
class JsonResponse:
    """A successfully parsed JSON response plus retry diagnostics."""

    url: str
    status_code: int
    payload: Any
    text: str
    attempts: int
    elapsed_ms: int
    response_headers: Mapping[str, str]


class HttpClient:
    """GET + JSON with bounded retries, pacing and a single User-Agent."""

    def __init__(
        self,
        *,
        user_agent: str,
        timeout: float = 20.0,
        retry: RetryPolicy | None = None,
        rate_limiter: RateLimiter | None = None,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
        rng: random.Random | None = None,
    ) -> None:
        self._retry = retry or RetryPolicy()
        self._rate_limiter = rate_limiter
        self._sleep = sleep
        self._rng = rng or random.Random()
        self._client = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            transport=transport,
            follow_redirects=True,
        )
        self.user_agent = user_agent

    @property
    def retry(self) -> RetryPolicy:
        return self._retry

    @property
    def rate_limiter(self) -> RateLimiter | None:
        return self._rate_limiter

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> HttpClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def get_json(self, url: str, params: Mapping[str, str] | None = None) -> JsonResponse:
        """GET ``url`` and return parsed JSON, retrying only transient failures."""
        started = time.monotonic()
        attempts = 0
        last_message = "request not attempted"
        last_reason = "retries_exhausted"
        last_status: int | None = None

        while attempts < self._retry.max_attempts:
            attempts += 1
            if self._rate_limiter is not None:
                self._rate_limiter.wait()

            retry_after: float | None = None
            try:
                response = self._client.get(url, params=dict(params or {}))
            except httpx.TimeoutException:
                last_message = f"timeout after {attempts} attempt(s) for {url}"
                last_reason = "timeout"
            except httpx.TransportError as exc:
                last_message = f"network error after {attempts} attempt(s) for {url}: {exc}"
                last_reason = "network"
            else:
                status = response.status_code
                if status in RETRYABLE_STATUS:
                    last_message = f"HTTP {status} from {url} after {attempts} attempt(s)"
                    last_reason = f"http_{status}"
                    last_status = status
                    retry_after = self._retry.retry_after_for(response.headers.get("Retry-After"))
                elif status >= 400:
                    snippet = response.text.strip()[:200]
                    raise ApiResponseError(
                        f"HTTP {status} from {url}: {snippet}",
                        reason=f"http_{status}",
                        status_code=status,
                    )
                else:
                    text = response.text
                    try:
                        payload = json.loads(text)
                    except json.JSONDecodeError as exc:
                        raise ApiResponseError(
                            f"invalid JSON from {url}: {exc}",
                            reason="invalid_json",
                            status_code=status,
                        ) from exc
                    return JsonResponse(
                        url=str(response.url),
                        status_code=status,
                        payload=payload,
                        text=text,
                        attempts=attempts,
                        elapsed_ms=int((time.monotonic() - started) * 1000),
                        response_headers=dict(response.headers),
                    )

            if attempts >= self._retry.max_attempts:
                break
            delay = self._retry.backoff_for(attempts, self._rng)
            if retry_after is not None:
                delay = max(delay, retry_after)
            self._sleep(delay)

        raise ApiRetryError(
            last_message,
            attempts=attempts,
            status_code=last_status,
            reason=last_reason,
        )
