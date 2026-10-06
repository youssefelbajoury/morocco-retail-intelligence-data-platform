"""Static Moroccan reference data: regions, cities, holidays, Ramadan periods.

All dates are fixed Gregorian observance dates for the generated period
(2021-01-01 .. 2024-12-31). They are simplified, documented approximations used
for synthetic seasonality only — not an authoritative religious calendar.
"""

from __future__ import annotations

from datetime import date

# 12 Moroccan regions (post-2015 administrative division) with demand weights
# used to distribute stores and customers. Weights are simplified population /
# purchasing-power approximations and are normalized at use time.
REGIONS: tuple[tuple[str, str, float], ...] = (
    ("R01", "Casablanca-Settat", 0.230),
    ("R02", "Rabat-Sale-Kenitra", 0.160),
    ("R03", "Marrakech-Safi", 0.120),
    ("R04", "Fes-Meknes", 0.110),
    ("R05", "Tanger-Tetouan-Al Hoceima", 0.090),
    ("R06", "Souss-Massa", 0.080),
    ("R07", "Oriental", 0.060),
    ("R08", "Beni Mellal-Khenifra", 0.050),
    ("R09", "Draa-Tafilalet", 0.030),
    ("R10", "Guelmim-Oued Noun", 0.020),
    ("R11", "Laayoune-Sakia El Hamra", 0.020),
    ("R12", "Dakhla-Oued Ed-Dahab", 0.010),
)

CITIES: dict[str, tuple[str, ...]] = {
    "R01": ("Casablanca", "Mohammedia", "El Jadida", "Settat", "Berrechid", "Benslimane"),
    "R02": ("Rabat", "Sale", "Kenitra", "Temara", "Khemisset"),
    "R03": ("Marrakech", "Safi", "Essaouira", "El Kelaa des Sraghna"),
    "R04": ("Fes", "Meknes", "Taza", "Sefrou", "El Hajeb"),
    "R05": ("Tanger", "Tetouan", "Al Hoceima", "Chefchaouen", "Ouazzane"),
    "R06": ("Agadir", "Taroudant", "Tiznit", "Oulad Teima"),
    "R07": ("Oujda", "Nador", "Berkane", "Jerada"),
    "R08": ("Beni Mellal", "Khenifra", "Azilal"),
    "R09": ("Errachidia", "Ouarzazate", "Tinghir", "Zagora"),
    "R10": ("Guelmim", "Tan-Tan", "Sidi Ifni", "Assa"),
    "R11": ("Laayoune", "Boujdour", "Es Semara"),
    "R12": ("Dakhla", "La Guera"),
}

# Simplified Moroccan public holidays (national + Islamic) for 2021-2024.
HOLIDAYS: dict[date, str] = {}


def _add(year: int, month: int, day: int, name: str) -> None:
    HOLIDAYS[date(year, month, day)] = name


def _build_holidays() -> None:
    for year in (2021, 2022, 2023, 2024):
        _add(year, 1, 1, "New Year")
        _add(year, 1, 11, "Independence Manifesto Day")
        _add(year, 5, 1, "Labour Day")
        _add(year, 7, 30, "Throne Day")
        _add(year, 8, 14, "Allegiance Day")
        _add(year, 8, 20, "Revolution of the King and the People")
        _add(year, 11, 6, "Green March Day")
        _add(year, 11, 18, "Independence Day")

    # Islamic observances (approximate observed dates)
    for day, name in (
        (date(2021, 5, 13), "Eid al-Fitr"),
        (date(2022, 5, 2), "Eid al-Fitr"),
        (date(2023, 4, 21), "Eid al-Fitr"),
        (date(2024, 4, 10), "Eid al-Fitr"),
        (date(2021, 7, 20), "Eid al-Adha"),
        (date(2022, 7, 9), "Eid al-Adha"),
        (date(2023, 6, 28), "Eid al-Adha"),
        (date(2024, 6, 16), "Eid al-Adha"),
        (date(2021, 8, 10), "Islamic New Year"),
        (date(2022, 7, 30), "Islamic New Year"),
        (date(2023, 7, 19), "Islamic New Year"),
        (date(2024, 7, 7), "Islamic New Year"),
        (date(2021, 10, 18), "Mawlid"),
        (date(2022, 10, 8), "Mawlid"),
        (date(2023, 9, 27), "Mawlid"),
        (date(2024, 9, 15), "Mawlid"),
    ):
        HOLIDAYS[day] = name


_build_holidays()

# Ramadan periods (start inclusive .. end inclusive); Eid al-Fitr follows.
RAMADAN_PERIODS: tuple[tuple[date, date], ...] = (
    (date(2021, 4, 13), date(2021, 5, 12)),
    (date(2022, 4, 2), date(2022, 5, 1)),
    (date(2023, 3, 23), date(2023, 4, 20)),
    (date(2024, 3, 11), date(2024, 4, 9)),
)

EID_AL_FITR_DATES: frozenset[date] = frozenset(
    (date(2021, 5, 13), date(2022, 5, 2), date(2023, 4, 21), date(2024, 4, 10))
)
EID_AL_ADHA_DATES: frozenset[date] = frozenset(
    (date(2021, 7, 20), date(2022, 7, 9), date(2023, 6, 28), date(2024, 6, 16))
)
