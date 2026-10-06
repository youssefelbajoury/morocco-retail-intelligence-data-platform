"""Calendar dimension for the generated period (seasonality / holiday flags)."""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd

from mri.synthetic.morocco import (
    EID_AL_ADHA_DATES,
    EID_AL_FITR_DATES,
    HOLIDAYS,
    RAMADAN_PERIODS,
)
from mri.synthetic.schema import ENTITY_COLUMNS, PERIOD_END, PERIOD_START

_WEEKDAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")

# Morocco: weekend = Saturday + Sunday (public sector convention since 2014).
_WEEKEND_DAYS = frozenset((5, 6))


def _season(month: int) -> str:
    if month in (12, 1, 2):
        return "winter"
    if month in (3, 4, 5):
        return "spring"
    if month in (6, 7, 8):
        return "summer"
    return "autumn"


def build_calendar() -> pd.DataFrame:
    """Build the daily calendar covering PERIOD_START .. PERIOD_END inclusive."""
    days: list[date] = []
    current = PERIOD_START
    while current <= PERIOD_END:
        days.append(current)
        current += timedelta(days=1)

    ramadan_days = set()
    for start, end in RAMADAN_PERIODS:
        cursor = start
        while cursor <= end:
            ramadan_days.add(cursor)
            cursor += timedelta(days=1)

    records = []
    for day in days:
        holiday = HOLIDAYS.get(day)
        records.append(
            {
                "date": day.isoformat(),
                "year": day.year,
                "month": day.month,
                "quarter": (day.month - 1) // 3 + 1,
                "iso_week": day.isocalendar().week,
                "weekday": day.weekday(),
                "weekday_name": _WEEKDAY_NAMES[day.weekday()],
                "is_weekend": day.weekday() in _WEEKEND_DAYS,
                "is_holiday": holiday is not None,
                "holiday_name": holiday,
                "is_ramadan": day in ramadan_days,
                "is_eid_al_fitr": day in EID_AL_FITR_DATES,
                "is_eid_al_adha": day in EID_AL_ADHA_DATES,
                "season": _season(day.month),
            }
        )

    frame = pd.DataFrame.from_records(records, columns=ENTITY_COLUMNS["calendar"])
    return frame
