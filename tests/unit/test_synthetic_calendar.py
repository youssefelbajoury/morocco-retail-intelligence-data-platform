"""Calendar dimension: coverage, flags and Moroccan seasonality markers."""

from __future__ import annotations

from datetime import date, timedelta

from mri.synthetic.calendar import build_calendar
from mri.synthetic.morocco import RAMADAN_PERIODS
from mri.synthetic.schema import ENTITY_COLUMNS, PERIOD_END, PERIOD_START


def test_calendar_covers_exact_period():
    calendar = build_calendar()
    assert list(calendar.columns) == list(ENTITY_COLUMNS["calendar"])
    assert calendar["date"].iloc[0] == PERIOD_START.isoformat()
    assert calendar["date"].iloc[-1] == PERIOD_END.isoformat()
    expected_days = (PERIOD_END - PERIOD_START).days + 1
    assert len(calendar) == expected_days


def test_calendar_is_contiguous_daily():
    calendar = build_calendar()
    dates = [date.fromisoformat(d) for d in calendar["date"]]
    assert all(
        later - earlier == timedelta(days=1)
        for earlier, later in zip(dates, dates[1:], strict=False)
    )


def test_weekend_is_saturday_and_sunday():
    calendar = build_calendar()
    weekend = calendar[calendar["is_weekend"]]
    assert set(weekend["weekday_name"]) == {"Saturday", "Sunday"}
    assert set(weekend["weekday"]) == {5, 6}


def test_ramadan_flag_matches_periods():
    calendar = build_calendar()
    flagged = sum(1 for v in calendar["is_ramadan"] if v)
    expected = 0
    for start, end in RAMADAN_PERIODS:
        expected += (end - start).days + 1
    assert flagged == expected


def test_holiday_name_present_iff_flagged():
    calendar = build_calendar()
    for row in calendar.itertuples(index=False):
        if row.is_holiday:
            assert isinstance(row.holiday_name, str) and row.holiday_name
        else:
            assert row.holiday_name is None


def test_eid_flags_present_each_year():
    calendar = build_calendar()
    for column in ("is_eid_al_fitr", "is_eid_al_adha"):
        assert sum(1 for v in calendar[column] if v) >= 4
    years = set(calendar["year"])
    assert years == {2021, 2022, 2023, 2024}


def test_season_values_are_valid():
    calendar = build_calendar()
    assert set(calendar["season"]) <= {"winter", "spring", "summer", "autumn"}
    assert set(calendar["quarter"]) == {1, 2, 3, 4}
