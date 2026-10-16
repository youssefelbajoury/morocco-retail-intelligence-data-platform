"""Browser end-to-end tests for the Superset dashboards and native filters.

Run explicitly with ``make e2e`` (or ``uv run pytest e2e``); they are excluded
from the default ``uv run pytest`` run via ``testpaths = ["tests"]``.

Each interaction asserts the *chart data* behind the visualisation (captured from
``/api/v1/chart/data``) rather than scraping formatted numbers from the DOM.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from playwright.sync_api import Page

from e2e.conftest import ChartDataCollector
from e2e.superset_page import (
    chart_slot_count,
    clear_all_filters,
    filter_names,
    open_dashboard,
    scalar_value,
    select_filter_value,
    wait_for_filter_options,
    wait_for_native_filters,
)

SALES = "Sales Performance"

#: (dashboard title, native filter name, KPI chart that the filter should move)
SELECT_FILTER_CASES: list[tuple[str, str, str]] = [
    (SALES, "Channel", "Net Revenue"),
    (SALES, "Category", "Net Revenue"),
    (SALES, "Region", "Net Revenue"),
    ("Inventory Health", "Stock Status", "Stock Value"),
    ("Executive Overview", "Category", "Revenue"),
    ("Procurement", "Category", "Purchase Cost"),
    ("Platform Health", "Source", "Pipeline Loads"),
]


def test_all_dashboards_render(
    page: Page,
    dashboards: dict[str, int],
    superset_url: str,
    artifacts: Path,
    chart_data: ChartDataCollector,
) -> None:
    assert dashboards, "no dashboards were returned by the Superset API"
    for title, dashboard_id in sorted(dashboards.items(), key=lambda item: item[1]):
        open_dashboard(page, superset_url, dashboard_id)
        wait_for_native_filters(page, expected_selects=1)
        names = filter_names(page)
        slots = chart_slot_count(page)
        assert names, f"dashboard {title!r} (id={dashboard_id}) rendered no native filters"
        assert slots > 0, f"dashboard {title!r} (id={dashboard_id}) rendered no charts"
        # give the charts a moment to paint before the artifact is captured
        page.wait_for_timeout(2500)
        page.screenshot(path=str(artifacts / f"dashboard-{dashboard_id}.png"), full_page=True)


@pytest.mark.parametrize(("dash_title", "filter_name", "kpi_chart"), SELECT_FILTER_CASES)
def test_select_filter_narrows_or_is_degenerate(
    page: Page,
    dashboards: dict[str, int],
    charts: dict[str, int],
    chart_data: ChartDataCollector,
    superset_url: str,
    dash_title: str,
    filter_name: str,
    kpi_chart: str,
) -> None:
    assert dash_title in dashboards, f"dashboard {dash_title!r} not found"
    assert kpi_chart in charts, f"chart {kpi_chart!r} not found"
    dashboard_id = dashboards[dash_title]
    kpi_id = charts[kpi_chart]

    open_dashboard(page, superset_url, dashboard_id)
    wait_for_native_filters(page, expected_selects=1)

    baseline = scalar_value(chart_data.wait_for(page, kpi_id))
    assert baseline > 0, f"{kpi_chart!r} baseline was not positive"

    options = wait_for_filter_options(page, filter_name)
    assert options, f"filter {filter_name!r} exposed no options"

    before_select = chart_data.snapshot()
    select_filter_value(page, filter_name, options[0])
    filtered = scalar_value(chart_data.wait_for(page, kpi_id, after=before_select))

    if len(options) == 1:
        # A single-valued filter cannot narrow the dataset: the honest expectation
        # is that the KPI is unchanged (documented degeneracy, e.g. Region).
        assert filtered == pytest.approx(baseline, abs=0.01), (
            f"{filter_name!r} has one option yet changed {kpi_chart!r}"
        )
    else:
        assert 0 < filtered < baseline, (
            f"selecting {options[0]!r} in {filter_name!r} did not narrow {kpi_chart!r} "
            f"({filtered} vs baseline {baseline})"
        )

    before_reset = chart_data.snapshot()
    clear_all_filters(page)
    restored = scalar_value(chart_data.wait_for(page, kpi_id, after=before_reset))
    assert restored == pytest.approx(baseline, abs=0.01), (
        f"clearing {filter_name!r} did not restore {kpi_chart!r} "
        f"({restored} vs baseline {baseline})"
    )
