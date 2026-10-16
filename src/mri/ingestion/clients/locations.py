"""WGS84 reference coordinates for the project's 12 Moroccan regions.

The synthetic reference data (:mod:`mri.synthetic.morocco`) defines regions and
cities but carries no coordinates, and weather requests require latitude and
longitude. Each region is therefore pinned to the well-known centre of its
chief city — coordinates are real public city-centre values, never invented,
and every city is reused verbatim from the project's ``CITIES`` table.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from mri.ingestion.exceptions import UsageError
from mri.synthetic.morocco import REGIONS


@dataclass(frozen=True)
class Location:
    """One weather-capable project location."""

    region_id: str
    region_name: str
    city: str
    latitude: float
    longitude: float

    @property
    def label(self) -> str:
        return f"{self.region_id}:{self.city}"

    def as_dict(self) -> dict[str, str | float]:
        return {
            "region_id": self.region_id,
            "region_name": self.region_name,
            "city": self.city,
            "latitude": self.latitude,
            "longitude": self.longitude,
        }


# region id -> (chief city, latitude, longitude), WGS84, city-centre precision.
_REGION_COORDINATES: dict[str, tuple[str, float, float]] = {
    "R01": ("Casablanca", 33.5731, -7.5898),
    "R02": ("Rabat", 34.0209, -6.8416),
    "R03": ("Marrakech", 31.6295, -7.9811),
    "R04": ("Fes", 34.0181, -5.0078),
    "R05": ("Tanger", 35.7595, -5.8340),
    "R06": ("Agadir", 30.4278, -9.5981),
    "R07": ("Oujda", 34.6814, -1.9086),
    "R08": ("Beni Mellal", 32.3373, -6.3498),
    "R09": ("Errachidia", 31.9314, -4.4244),
    "R10": ("Guelmim", 28.9870, -10.0574),
    "R11": ("Laayoune", 27.1536, -13.2033),
    "R12": ("Dakhla", 23.6848, -15.9580),
}

_REGION_NAMES = {region_id: name for region_id, name, _weight in REGIONS}

LOCATIONS: tuple[Location, ...] = tuple(
    Location(
        region_id=region_id,
        region_name=_REGION_NAMES[region_id],
        city=city,
        latitude=latitude,
        longitude=longitude,
    )
    for region_id, (city, latitude, longitude) in _REGION_COORDINATES.items()
)

# Every accepted selector (region id, region name or city), lower-cased.
_SELECTOR_INDEX: dict[str, Location] = {}
for _location in LOCATIONS:
    _SELECTOR_INDEX[_location.region_id.lower()] = _location
    _SELECTOR_INDEX[_location.region_name.lower()] = _location
    _SELECTOR_INDEX[_location.city.lower()] = _location


def resolve_locations(selectors: Sequence[str]) -> list[Location]:
    """Resolve CLI selectors (region ids/names or cities); empty means all.

    Selections are returned in project region order (R01..R12) and
    de-duplicated, so a run is deterministic regardless of argument order.
    """
    if not selectors:
        return list(LOCATIONS)
    picked: dict[str, Location] = {}
    unknown: list[str] = []
    for selector in selectors:
        location = _SELECTOR_INDEX.get(selector.strip().lower())
        if location is None:
            unknown.append(selector)
        else:
            picked[location.region_id] = location
    if unknown:
        known = ", ".join(sorted({loc.region_id for loc in LOCATIONS}))
        raise UsageError(
            f"unknown --location value(s): {', '.join(unknown)}; "
            f"use region ids ({known}), region names or chief cities"
        )
    return [location for location in LOCATIONS if location.region_id in picked]
