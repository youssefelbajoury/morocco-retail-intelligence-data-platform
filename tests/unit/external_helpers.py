"""Shared helpers for the external-ingestion unit tests.

Everything here is offline: HTTP goes through ``httpx.MockTransport`` and
sleeping/clocks are faked, so the suite never waits and never touches the
network.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

import httpx

from mri.ingestion.clients.http import HttpClient, RateLimiter, RetryPolicy

TEST_USER_AGENT = "MoroccoRetailIntelligence-Tests/0.1.0 (tests@example.invalid)"


@dataclass
class FakeSleep:
    """Records every requested sleep and advances a fake monotonic clock."""

    slept: list[float] = field(default_factory=list)
    now: float = 0.0

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds

    def clock(self) -> float:
        return self.now


def make_http(
    handler: object,
    *,
    fake: FakeSleep | None = None,
    max_attempts: int = 4,
    min_interval: float = 0.0,
    jitter_ratio: float = 0.0,
) -> HttpClient:
    """Build an :class:`HttpClient` wired to a mock transport and fake clock."""
    fake = fake or FakeSleep()
    limiter = (
        RateLimiter(min_interval, sleep=fake.sleep, clock=fake.clock) if min_interval else None
    )
    return HttpClient(
        user_agent=TEST_USER_AGENT,
        timeout=5.0,
        retry=RetryPolicy(max_attempts=max_attempts, jitter_ratio=jitter_ratio),
        rate_limiter=limiter,
        transport=httpx.MockTransport(handler),  # type: ignore[arg-type]
        sleep=fake.sleep,
        rng=random.Random(7),
    )


def json_response(
    request: httpx.Request,
    data: object,
    status: int = 200,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(status, json=data, headers=headers or {}, request=request)


def text_response(request: httpx.Request, text: str, status: int = 200) -> httpx.Response:
    return httpx.Response(status, text=text, request=request)
