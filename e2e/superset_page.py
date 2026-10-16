"""Low-level Superset dashboard/filter interactions used by the e2e tests.

The selectors here are the stable ``data-test`` hooks Superset ships on its filter
bar (``filter-bar__apply-button`` etc.); options themselves render into an Ant
Design portal (``.ant-select-dropdown``), so option reading/clicks are scoped to
the *visible* dropdown to avoid leaking between filters.
"""

from __future__ import annotations

import time

from playwright.sync_api import Locator, Page

QUERY_TIMEOUT_S = 180


def open_dashboard(page: Page, base_url: str, dashboard_id: int) -> None:
    page.goto(f"{base_url}/superset/dashboard/{dashboard_id}/", wait_until="domcontentloaded")


def wait_for_native_filters(page: Page, expected_selects: int = 1, timeout: float = 120) -> None:
    """Wait until the filter bar is mounted and its Select controls have rendered."""
    page.wait_for_selector("[data-test='filter-control-name']", timeout=int(timeout * 1000))
    deadline = time.time() + timeout
    while time.time() < deadline:
        if page.locator("[data-test='filter-bar'] .ant-select").count() >= expected_selects:
            return
        page.wait_for_timeout(1500)
    raise AssertionError("native filter Select controls did not render")


def filter_names(page: Page) -> list[str]:
    names = page.locator("[data-test='filter-control-name']")
    return [names.nth(i).inner_text().strip() for i in range(names.count())]


def _filter_form(page: Page, name: str) -> Locator:
    return page.locator(f"form:has([data-test='filter-control-name']:has-text('{name}'))")


def chart_slot_count(page: Page) -> int:
    for selector in (
        "[data-test='chart-container']",
        ".dashboard-component-chart-holder",
        "[data-test='chart-header']",
    ):
        count = page.locator(selector).count()
        if count:
            return count
    return 0


def _visible_dropdown(page: Page) -> Locator:
    return page.locator(".ant-select-dropdown:visible").last


def filter_option_values(page: Page, name: str) -> list[str]:
    """Open the named Select filter and return its option labels (exactly)."""
    _filter_form(page, name).locator(".ant-select").first.click()
    page.wait_for_timeout(1500)
    options = _visible_dropdown(page).locator(".ant-select-item-option")
    values = [options.nth(i).inner_text().strip() for i in range(options.count())]
    page.keyboard.press("Escape")
    page.wait_for_timeout(300)
    return values


def wait_for_filter_options(
    page: Page, name: str, timeout: float = 120, polls_between: float = 2.0
) -> list[str]:
    """Poll until the Select filter has real option values (not the empty ``<NULL>``)."""
    deadline = time.time() + timeout
    values: list[str] = []
    while time.time() < deadline:
        values = filter_option_values(page, name)
        if values and values != ["<NULL>"]:
            return values
        page.wait_for_timeout(int(polls_between * 1000))
    return values


def _click_apply(page: Page) -> None:
    apply_button = page.locator("[data-test='filter-bar__apply-button']")
    if apply_button.count() and apply_button.first.is_enabled():
        apply_button.first.click()
        page.wait_for_timeout(800)


def select_filter_value(page: Page, name: str, value: str, apply: bool = True) -> None:
    """Select an exact option label in the named filter and (optionally) Apply."""
    _filter_form(page, name).locator(".ant-select").first.click()
    page.wait_for_timeout(1200)
    options = _visible_dropdown(page).locator(".ant-select-item-option")
    target: Locator | None = None
    for i in range(options.count()):
        if options.nth(i).inner_text().strip() == value:
            target = options.nth(i)
            break
    assert target is not None, f"option {value!r} not found in filter {name!r}"
    target.click()
    page.wait_for_timeout(500)
    if apply:
        _click_apply(page)


def clear_all_filters(page: Page, apply: bool = True) -> None:
    clear_button = page.locator("[data-test='filter-bar__clear-button']")
    clear_button.first.click()
    page.wait_for_timeout(1200)
    if apply:
        _click_apply(page)


def scalar_value(data: object) -> float:
    """Extract the single numeric value from a ``big_number_total`` chart payload."""
    assert isinstance(data, list) and data, f"unexpected KPI payload: {data!r}"
    row = data[0]
    assert isinstance(row, dict) and len(row) == 1, f"expected one metric in {row!r}"
    return float(next(iter(row.values())))
