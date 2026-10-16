"""Fixtures for the Superset browser end-to-end suite.

These tests drive a real browser (Playwright, using the locally installed Edge via
``channel="msedge"``) against the live Superset instance. They live in ``e2e/``
instead of ``tests/`` on purpose: the default ``uv run pytest`` run (``testpaths =
["tests"]``) must never launch a browser or require the BI stack.

Credentials are read from the git-ignored ``.env`` and never printed.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import pytest
from playwright.sync_api import Browser, Page, Response, sync_playwright

REPO_ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = Path(__file__).resolve().parent / "artifacts"

#: how long we allow the (virtual) datasets to answer a dashboard query
QUERY_TIMEOUT_S = 180


def load_env() -> dict[str, str]:
    """Read ``.env`` (path overridable with ``_MRI_ENV_PATH``) into a plain dict."""
    path = Path(os.environ.get("_MRI_ENV_PATH", str(REPO_ROOT / ".env")))
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _, value = stripped.partition("=")
            env[key.strip()] = value
    return env


def ui_login(page: Page, base_url: str, user: str, password: str) -> None:
    """Log into the Superset web UI.

    The login form is a React/Ant Design form: ``#username``/``#password`` plus a
    ``button[type=submit]`` with no ``name`` attribute. Playwright's default
    auto-wait for navigation can hang on this form, so we click without waiting
    and then poll the URL until we leave ``/login``.
    """
    page.goto(f"{base_url}/login/", wait_until="domcontentloaded")
    if "/login" not in page.url:
        return
    page.fill("#username", user)
    page.fill("#password", password)
    page.click("button[type=submit]", no_wait_after=True)
    deadline = time.time() + 90
    while time.time() < deadline and "/login" in page.url:
        page.wait_for_timeout(500)
    assert "/login" not in page.url, "Superset UI login did not succeed"


class ChartDataCollector:
    """Records the JSON payload of every ``/api/v1/chart/data`` response.

    Keying on ``slice_id`` (present in the request URL) lets a test assert the
    *data* behind a rendered chart without scraping formatted numbers out of the
    DOM, which is far more stable than matching display text.
    """

    def __init__(self) -> None:
        self.records: list[tuple[int | None, Any]] = []

    def __call__(self, response: Response) -> None:
        if "/api/v1/chart/data" not in response.url:
            return
        slice_id: int | None = None
        raw = parse_qs(urlsplit(response.url).query).get("form_data")
        if raw:
            try:
                parsed = json.loads(raw[0])
            except (ValueError, TypeError):
                parsed = None
            if isinstance(parsed, dict):
                slice_id = parsed.get("slice_id")
        try:
            data = response.json()["result"][0]["data"]
        except Exception:  # noqa: BLE001 - a partial/errored response is simply skipped
            return
        if isinstance(data, list):
            self.records.append((slice_id, data))

    def snapshot(self) -> int:
        """Number of records seen so far; pass the result to ``wait_for`` later."""
        return len(self.records)

    def latest(self, slice_id: int, after: int = 0) -> Any:
        """Most recent payload for ``slice_id`` seen at index ``after`` or later."""
        found: Any = None
        for index, (sid, data) in enumerate(self.records):
            if index >= after and sid == slice_id:
                found = data
        return found

    def wait_for(
        self, page: Page, slice_id: int, after: int = 0, timeout: float = QUERY_TIMEOUT_S
    ) -> Any:
        """Block until a chart payload for ``slice_id`` arrives after ``after``."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            data = self.latest(slice_id, after)
            if data:
                return data
            page.wait_for_timeout(500)
        raise AssertionError(f"no chart data for slice_id={slice_id} within {timeout}s")


@pytest.fixture(scope="session")
def env() -> dict[str, str]:
    return load_env()


@pytest.fixture(scope="session")
def superset_url(env: dict[str, str]) -> str:
    return env.get("SUPERSET_URL") or f"http://localhost:{env.get('SUPERSET_PORT', '8088')}"


@pytest.fixture(scope="session")
def superset_available(superset_url: str) -> bool:
    import urllib.error
    import urllib.request

    try:
        with urllib.request.urlopen(f"{superset_url}/health", timeout=10) as response:
            return bool(response.status == 200)
    except (urllib.error.URLError, TimeoutError, ValueError):
        return False


@pytest.fixture(scope="session")
def browser(superset_available: bool) -> Iterator[Browser]:
    if not superset_available:
        pytest.skip("Superset is not reachable — start it with 'make superset-up'")
    channel = os.environ.get("MRI_E2E_BROWSER_CHANNEL", "msedge")
    headless = os.environ.get("MRI_E2E_HEADLESS", "1") != "0"
    with sync_playwright() as playwright:
        launched = playwright.chromium.launch(channel=channel or None, headless=headless)
        try:
            yield launched
        finally:
            launched.close()


@pytest.fixture(scope="session")
def page(browser: Browser, env: dict[str, str], superset_url: str) -> Iterator[Page]:
    """A logged-in page shared by the whole session (each test navigates fresh)."""
    context = browser.new_context(viewport={"width": 1600, "height": 1000})
    new_page = context.new_page()
    user = env.get("SUPERSET_ADMIN_USER")
    password = env.get("SUPERSET_ADMIN_PASSWORD")
    if not user or not password:
        pytest.skip("SUPERSET_ADMIN_USER/SUPERSET_ADMIN_PASSWORD missing from .env")
    ui_login(new_page, superset_url, user, password)
    try:
        yield new_page
    finally:
        context.close()


@pytest.fixture(scope="session")
def chart_data(page: Page) -> ChartDataCollector:
    collector = ChartDataCollector()
    page.on("response", collector)
    return collector


@pytest.fixture(scope="session")
def charts(page: Page, superset_url: str) -> dict[str, int]:
    """Map ``slice_name -> slice id`` so tests never hardcode chart ids."""
    payload = page.request.get(f"{superset_url}/api/v1/chart/?q=(page_size:1000)").json()
    return {item["slice_name"]: item["id"] for item in payload.get("result", [])}


@pytest.fixture(scope="session")
def dashboards(page: Page, superset_url: str) -> dict[str, int]:
    """Map ``dashboard_title -> dashboard id`` so tests never hardcode dashboard ids."""
    payload = page.request.get(f"{superset_url}/api/v1/dashboard/?q=(page_size:1000)").json()
    return {item["dashboard_title"]: item["id"] for item in payload.get("result", [])}


@pytest.fixture(scope="session")
def artifacts() -> Path:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    return ARTIFACTS
