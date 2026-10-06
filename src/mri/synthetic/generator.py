"""Deterministic synthetic data generation for the Morocco Retail Intelligence platform.

This is *controlled synthetic data*. Realistic relationships are modelled
explicitly (product popularity, regional demand weights, store traffic
differences, loyalty-driven customer frequency, weekday/holiday/Ramadan
seasonality, promotion exposure with quantity lift, capped discounts and
non-negative margin rules, rolling replenishment driving purchases and
inventory snapshots), but the dataset is NOT statistically equivalent to a
real enterprise and must never be presented as observed production data.

Determinism: every random draw comes from component generators spawned by
``numpy.random.SeedSequence(seed)`` in a fixed component order, so the same
``seed`` and ``scale`` always produce the same logical dataset.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd

from mri.synthetic.calendar import build_calendar
from mri.synthetic.morocco import CITIES, REGIONS
from mri.synthetic.profiles import ScaleProfile
from mri.synthetic.schema import ENTITY_COLUMNS, PERIOD_END, PERIOD_START

PERIOD_DAYS = (PERIOD_END - PERIOD_START).days + 1

COMPONENT_ORDER: tuple[str, ...] = (
    "world",
    "stores",
    "products",
    "customers",
    "promotions",
    "sales",
    "purchases",
    "inventory",
)

DAY_ISO: list[str] = [(PERIOD_START + timedelta(days=i)).isoformat() for i in range(PERIOD_DAYS)]
DATE_ORD: dict[str, int] = {iso: i for i, iso in enumerate(DAY_ISO)}
DAY_ISO_ARR: np.ndarray = np.asarray(DAY_ISO, dtype=object)

CHANNELS: tuple[tuple[str, str, str, float], ...] = (
    ("CH01", "In-store", "physical", 0.78),
    ("CH02", "E-commerce", "digital", 0.14),
    ("CH03", "Mobile app", "digital", 0.08),
)

_STORE_FORMATS: tuple[tuple[str, float, int, int, float], ...] = (
    # format, share, min_sqm, max_sqm, traffic factor
    ("hypermarket", 0.12, 4_500, 9_000, 2.2),
    ("supermarket", 0.43, 800, 2_500, 1.3),
    ("convenience", 0.35, 180, 650, 0.7),
    ("cash_and_carry", 0.10, 1_200, 4_000, 1.5),
)

_STORE_BRANDS: tuple[str, ...] = (
    "Atlas Market",
    "Souk Market",
    "Dari Market",
    "Riad Market",
    "Karam Market",
    "Fennec Market",
    "Nour Market",
    "Chaouia Market",
)

# category code, catalog share, turnover factor
_CATEGORY_MIX: tuple[tuple[str, float, float], ...] = (
    ("epicerie", 0.28, 1.0),
    ("boissons", 0.15, 1.25),
    ("produits_laitiers", 0.12, 1.3),
    ("frais", 0.15, 1.5),
    ("menage", 0.14, 0.8),
    ("beaute", 0.09, 0.6),
    ("snack", 0.07, 1.1),
)

# category, subcategory, label, cost_lo, cost_hi, margin_lo, margin_hi, packs
_SUBCATS: tuple[tuple[str, str, str, float, float, float, float, tuple[str, ...]], ...] = (
    ("epicerie", "pates", "Pâtes", 4, 30, 0.15, 0.26, ("500g", "1kg")),
    ("epicerie", "riz", "Riz", 6, 60, 0.14, 0.24, ("1kg", "5kg")),
    ("epicerie", "huile", "Huile", 18, 90, 0.10, 0.18, ("1L", "5L")),
    ("epicerie", "sucre", "Sucre", 5, 25, 0.10, 0.16, ("1kg", "2kg")),
    ("epicerie", "the", "Thé", 6, 45, 0.25, 0.38, ("100g", "200g")),
    ("epicerie", "cafe", "Café", 12, 90, 0.22, 0.35, ("250g", "500g")),
    ("epicerie", "farine", "Farine", 4, 20, 0.10, 0.16, ("1kg", "5kg")),
    ("epicerie", "conserves", "Conserves", 6, 40, 0.20, 0.32, ("400g", "800g")),
    ("boissons", "eaux", "Eau minérale", 3, 20, 0.15, 0.25, ("1.5L", "6x1.5L")),
    ("boissons", "jus", "Jus", 5, 45, 0.18, 0.30, ("250ml", "1L")),
    ("boissons", "sodas", "Soda", 6, 40, 0.20, 0.32, ("330ml", "1.5L")),
    ("produits_laitiers", "lait", "Lait", 5, 40, 0.08, 0.15, ("1L", "6x1L")),
    ("produits_laitiers", "yaourt", "Yaourt", 6, 45, 0.12, 0.20, ("4x125g", "12x125g")),
    ("produits_laitiers", "fromage", "Fromage", 8, 70, 0.12, 0.22, ("200g", "400g")),
    ("produits_laitiers", "beurre", "Beurre", 7, 50, 0.10, 0.18, ("250g", "500g")),
    ("frais", "fruits", "Fruits & légumes", 4, 30, 0.18, 0.32, ("1kg", "500g")),
    ("frais", "volaille", "Volaille", 25, 110, 0.12, 0.22, ("1kg", "2kg")),
    ("frais", "poisson", "Poisson", 30, 160, 0.12, 0.24, ("1kg", "500g")),
    ("frais", "boulangerie", "Pain", 1, 12, 0.25, 0.40, ("piece", "6 pieces")),
    ("menage", "lessive", "Lessive", 15, 130, 0.25, 0.38, ("2kg", "5L")),
    ("menage", "vaisselle", "Liquide vaisselle", 6, 55, 0.28, 0.40, ("500ml", "1L")),
    ("menage", "nettoyant", "Nettoyant", 7, 60, 0.26, 0.38, ("750ml", "1L")),
    ("menage", "papier", "Papier menager", 8, 70, 0.24, 0.36, ("6 rolls", "8 rolls")),
    ("beaute", "soins_corps", "Soin corps", 15, 140, 0.35, 0.50, ("250ml", "500ml")),
    ("beaute", "cheveux", "Soin cheveux", 12, 110, 0.35, 0.50, ("200ml", "400ml")),
    ("beaute", "bebe", "Hygiene bebe", 10, 90, 0.30, 0.45, ("petit", "grand")),
    ("snack", "biscuits", "Biscuits", 4, 45, 0.28, 0.42, ("150g", "pack")),
    ("snack", "chocolat", "Chocolat", 5, 55, 0.30, 0.45, ("100g", "200g")),
    ("snack", "confiseries", "Confiseries", 5, 60, 0.30, 0.45, ("200g", "500g")),
)

_PRODUCT_BRANDS: tuple[str, ...] = (
    "Atlas",
    "Dari",
    "Riad",
    "Zellige",
    "Andalous",
    "Sahara",
    "Nour",
    "Chaouia",
    "Karam",
    "Détroit",
)

_PROMO_TYPES: tuple[tuple[str, float, float, float], ...] = (
    # type, discount_lo, discount_hi, catalog share
    ("remise", 0.10, 0.30, 0.45),
    ("prix_promo", 0.15, 0.25, 0.25),
    ("bogo", 0.40, 0.40, 0.15),
    ("fidelite", 0.05, 0.15, 0.10),
    ("bundle", 0.18, 0.28, 0.05),
)

_LOYALTY_TIERS: tuple[tuple[str, float, float], ...] = (
    # tier, catalog share, purchase frequency multiplier
    ("bronze", 0.52, 1.0),
    ("silver", 0.30, 1.3),
    ("gold", 0.13, 1.8),
    ("platinum", 0.05, 2.5),
)

_GENDER_WEIGHTS: tuple[tuple[str, float], ...] = (("M", 0.48), ("F", 0.50), ("U", 0.02))

_WEEKDAY_FACTORS: tuple[float, ...] = (0.90, 0.92, 0.95, 1.02, 1.25, 1.30, 1.06)
_MONTH_FACTORS: tuple[float, ...] = (
    0.94,
    0.95,
    1.0,
    1.0,
    1.02,
    1.0,
    1.10,
    1.12,
    0.98,
    1.0,
    1.02,
    1.15,
)
_HOLIDAY_FACTOR = 1.30
_RAMADAN_FACTOR = 1.15
_EID_FITR_FACTOR = 1.60
_EID_ADHA_FACTOR = 1.35

_HOURS: tuple[int, ...] = tuple(range(8, 24))
_HOUR_WEEKDAY: tuple[float, ...] = (2, 3, 4, 5, 7, 6, 5, 5, 7, 10, 11, 11, 9, 6, 3, 1)
_HOUR_WEEKEND: tuple[float, ...] = (1, 2, 3, 5, 7, 7, 6, 7, 8, 9, 10, 10, 9, 7, 4, 2)
_HOUR_RAMADAN: tuple[float, ...] = (1, 1, 2, 3, 4, 4, 5, 6, 7, 6, 8, 14, 16, 14, 7, 2)

_QTY_LEVELS: tuple[int, ...] = (1, 2, 3, 4, 5)
_QTY_WEIGHTS: tuple[float, ...] = (0.66, 0.21, 0.075, 0.035, 0.02)
_BASKET_LEVELS: tuple[int, ...] = (1, 2, 3, 4, 5)
_BASKET_WEIGHTS: tuple[float, ...] = (0.55, 0.25, 0.12, 0.05, 0.03)
_BASKET_MEAN = sum(lv * w for lv, w in zip(_BASKET_LEVELS, _BASKET_WEIGHTS, strict=True))

_PROMO_EXPOSURE = 0.65
_PROMO_LIFT_PROB = 0.40
_ANONYMOUS_RATE = 0.10
_SAME_REGION_RATE = 0.70
_PREF_AFFINITY_RATE = 0.60
_INACTIVE_STORE_RATE = 0.04
_INACTIVE_CUSTOMER_RATE = 0.02
_MAX_DISCOUNT_OF_MARGIN = 0.90
_REPLENISH_TARGET_FACTOR = 2.0
_REPLENISH_HISTORY_WINDOWS = 6

_PRE_PERIOD_START = date(2015, 1, 1)
_PRE_PERIOD_DAYS = (date(2020, 12, 31) - _PRE_PERIOD_START).days + 1
_SIGNUP_START = date(2017, 1, 1)
_SIGNUP_DAYS = (date(2024, 6, 30) - _SIGNUP_START).days + 1


def spawn_rngs(seed: int) -> dict[str, np.random.Generator]:
    """Spawn one independent generator per fixed component, derived from ``seed``."""
    children = np.random.SeedSequence(seed).spawn(len(COMPONENT_ORDER))
    return {
        name: np.random.default_rng(child)
        for name, child in zip(COMPONENT_ORDER, children, strict=True)
    }


def _cume(weights: np.ndarray) -> np.ndarray:
    total = float(weights.sum())
    if total <= 0:
        raise ValueError("weights must sum to a positive value")
    return np.cumsum(weights / total)


def _search(cume: np.ndarray, draws: np.ndarray) -> np.ndarray:
    """Inverse-CDF sampling; ``side='right'`` skips zero-weight positions."""
    result: np.ndarray = np.searchsorted(cume, draws * cume[-1], side="right")
    return result


def _basket_sizes(rng: np.random.Generator, total_lines: int) -> np.ndarray:
    """Draw basket sizes for transactions summing exactly to ``total_lines``."""
    n_tx = max(1, int(round(total_lines / _BASKET_MEAN)))
    sizes = rng.choice(np.asarray(_BASKET_LEVELS), size=n_tx, p=np.asarray(_BASKET_WEIGHTS))
    diff = int(total_lines - int(sizes.sum()))
    if diff > 0:
        idx = rng.integers(0, n_tx, size=diff)
        np.add.at(sizes, idx, 1)
    elif diff < 0:
        need = -diff
        while need > 0:
            eligible = np.flatnonzero(sizes > 1)
            if eligible.size == 0:
                raise RuntimeError("cannot fit basket sizes to requested line count")
            take = min(need, int(eligible.size))
            chosen = rng.choice(eligible, size=take, replace=False)
            sizes[chosen] -= 1
            need -= take
    return sizes.astype(np.int64, copy=False)


def _build_day_weights(calendar: pd.DataFrame) -> np.ndarray:
    factors = np.asarray(_WEEKDAY_FACTORS, dtype=np.float64)[calendar["weekday"].to_numpy()]
    factors = (
        factors * np.asarray(_MONTH_FACTORS, dtype=np.float64)[calendar["month"].to_numpy() - 1]
    )
    factors = np.where(calendar["is_holiday"].to_numpy(), factors * _HOLIDAY_FACTOR, factors)
    factors = np.where(calendar["is_ramadan"].to_numpy(), factors * _RAMADAN_FACTOR, factors)
    factors = np.where(calendar["is_eid_al_fitr"].to_numpy(), factors * _EID_FITR_FACTOR, factors)
    factors = np.where(calendar["is_eid_al_adha"].to_numpy(), factors * _EID_ADHA_FACTOR, factors)
    return _cume(factors)


def _build_ref_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    region_weights = np.asarray([w for _, _, w in REGIONS], dtype=np.float64)
    regions = pd.DataFrame(
        {
            "region_id": [r[0] for r in REGIONS],
            "region_name": [r[1] for r in REGIONS],
            "demand_weight": np.round(region_weights / region_weights.sum(), 6),
        },
        columns=ENTITY_COLUMNS["regions"],
    )
    channel_weights = np.asarray([c[3] for c in CHANNELS], dtype=np.float64)
    channels = pd.DataFrame(
        {
            "channel_id": [c[0] for c in CHANNELS],
            "channel_name": [c[1] for c in CHANNELS],
            "channel_type": [c[2] for c in CHANNELS],
            "demand_weight": np.round(channel_weights / channel_weights.sum(), 6),
        },
        columns=ENTITY_COLUMNS["channels"],
    )
    return regions, channels


def _build_stores(rng: np.random.Generator, n: int) -> pd.DataFrame:
    region_weights = np.asarray([w for _, _, w in REGIONS], dtype=np.float64)
    region_idx = _search(_cume(region_weights), rng.random(n))
    fmt_idx = rng.choice(len(_STORE_FORMATS), size=n, p=[f[1] for f in _STORE_FORMATS])
    city_pick = rng.integers(0, 10_000, size=n)
    size_sqm = np.empty(n, dtype=np.int64)
    for i in range(n):
        fmt = _STORE_FORMATS[int(fmt_idx[i])]
        size_sqm[i] = int(rng.integers(fmt[2], fmt[3] + 1))
    open_off = rng.integers(0, _PRE_PERIOD_DAYS, size=n)
    open_dates = (
        np.datetime64(_PRE_PERIOD_START.isoformat()) + open_off.astype("timedelta64[D]")
    ).astype(str)
    active = rng.random(n) >= _INACTIVE_STORE_RATE

    records = []
    for i in range(n):
        region_id = REGIONS[int(region_idx[i])][0]
        cities = CITIES[region_id]
        city = cities[int(city_pick[i]) % len(cities)]
        fmt = _STORE_FORMATS[int(fmt_idx[i])]
        records.append(
            {
                "store_id": f"ST{i + 1:03d}",
                "store_name": f"{_STORE_BRANDS[i % len(_STORE_BRANDS)]} {city} {i + 1:02d}",
                "city": city,
                "region_id": region_id,
                "store_format": fmt[0],
                "size_sqm": int(size_sqm[i]),
                "open_date": str(open_dates[i]),
                "is_active": bool(active[i]),
            }
        )
    return pd.DataFrame.from_records(records, columns=ENTITY_COLUMNS["stores"])


def _build_products(rng: np.random.Generator, n: int) -> pd.DataFrame:
    cat_codes = [c[0] for c in _CATEGORY_MIX]
    cat_idx = rng.choice(len(_CATEGORY_MIX), size=n, p=[c[1] for c in _CATEGORY_MIX])
    subcat_pool: dict[str, list[int]] = {}
    for pos, spec in enumerate(_SUBCATS):
        subcat_pool.setdefault(spec[0], []).append(pos)
    subcat_idx = np.empty(n, dtype=np.int64)
    for i in range(n):
        pool = subcat_pool[cat_codes[int(cat_idx[i])]]
        subcat_idx[i] = pool[int(rng.integers(0, len(pool)))]

    # Popularity: zipf rank over a shuffled order, scaled by category turnover.
    order = rng.permutation(n)
    zipf = np.empty(n, dtype=np.float64)
    zipf[order] = 1.0 / np.arange(1, n + 1, dtype=np.float64) ** 0.85
    turnover = {c[0]: c[2] for c in _CATEGORY_MIX}
    pop = np.asarray([turnover[_SUBCATS[int(s)][0]] for s in subcat_idx], dtype=np.float64) * zipf

    cost_lo = np.asarray([s[3] for s in _SUBCATS], dtype=np.float64)[subcat_idx]
    cost_hi = np.asarray([s[4] for s in _SUBCATS], dtype=np.float64)[subcat_idx]
    margin_lo = np.asarray([s[5] for s in _SUBCATS], dtype=np.float64)[subcat_idx]
    margin_hi = np.asarray([s[6] for s in _SUBCATS], dtype=np.float64)[subcat_idx]
    cost = np.round(np.exp(rng.uniform(np.log(cost_lo), np.log(cost_hi))), 2)
    margin = rng.uniform(margin_lo, margin_hi)
    price = np.round(cost / (1.0 - margin), 2)
    margin_pct = np.round((price - cost) / price, 4)
    brand_pick = rng.integers(0, len(_PRODUCT_BRANDS), size=n)
    pack_pick = rng.integers(0, 2, size=n)
    launch_off = rng.integers(0, _PRE_PERIOD_DAYS, size=n)
    launch_dates = (
        np.datetime64(_PRE_PERIOD_START.isoformat()) + launch_off.astype("timedelta64[D]")
    ).astype(str)

    records = []
    used_names: set[str] = set()
    dup_counter: dict[str, int] = {}
    for i in range(n):
        spec = _SUBCATS[int(subcat_idx[i])]
        brand = _PRODUCT_BRANDS[int(brand_pick[i])]
        pack = str(spec[7][int(pack_pick[i])])
        base_name = f"{brand} {spec[2]} {pack}"
        name = base_name
        if name in used_names:
            dup_counter[base_name] = dup_counter.get(base_name, 1) + 1
            name = f"{base_name} #{dup_counter[base_name]}"
        used_names.add(name)
        records.append(
            {
                "product_id": f"P{i + 1:06d}",
                "product_name": name,
                "category": spec[0],
                "subcategory": spec[1],
                "brand": brand,
                "unit_cost_mad": float(cost[i]),
                "unit_price_mad": float(price[i]),
                "margin_pct": float(margin_pct[i]),
                "launch_date": str(launch_dates[i]),
                "is_active": True,
            }
        )
    frame = pd.DataFrame.from_records(records, columns=ENTITY_COLUMNS["products"])
    frame.attrs["popularity"] = pop
    return frame


def _build_customers(rng: np.random.Generator, n: int) -> pd.DataFrame:
    region_weights = np.asarray([w for _, _, w in REGIONS], dtype=np.float64)
    region_idx = _search(_cume(region_weights), rng.random(n))
    signup_off = rng.integers(0, _SIGNUP_DAYS, size=n)
    signup_ord = (_SIGNUP_START - PERIOD_START).days + signup_off
    signup_dates = (
        np.datetime64(PERIOD_START.isoformat()) + signup_ord.astype("timedelta64[D]")
    ).astype(str)
    birth_year = np.clip(np.rint(rng.normal(1985, 15, size=n)), 1945, 2006).astype(np.int64)
    gender = rng.choice([g for g, _ in _GENDER_WEIGHTS], size=n, p=[w for _, w in _GENDER_WEIGHTS])
    seg_roll = rng.random(n)
    segment = np.where(
        birth_year >= 2000,
        "jeune",
        np.where(birth_year <= 1965, "senior", np.where(seg_roll < 0.55, "famille", "actif")),
    )
    tier_idx = rng.choice(len(_LOYALTY_TIERS), size=n, p=[t[1] for t in _LOYALTY_TIERS])
    channel_idx = _search(_cume(np.asarray([c[3] for c in CHANNELS])), rng.random(n))
    freq = rng.lognormal(mean=0.0, sigma=1.2, size=n)
    tier_mult = np.asarray([t[2] for t in _LOYALTY_TIERS], dtype=np.float64)
    freq = freq * tier_mult[tier_idx]
    active = rng.random(n) >= _INACTIVE_CUSTOMER_RATE

    records = []
    for i in range(n):
        records.append(
            {
                "customer_id": f"CU{i + 1:06d}",
                "signup_date": str(signup_dates[i]),
                "region_id": REGIONS[int(region_idx[i])][0],
                "birth_year": int(birth_year[i]),
                "gender": str(gender[i]),
                "segment": str(segment[i]),
                "loyalty_tier": _LOYALTY_TIERS[int(tier_idx[i])][0],
                "preferred_channel_id": CHANNELS[int(channel_idx[i])][0],
                "is_active": bool(active[i]),
            }
        )
    frame = pd.DataFrame.from_records(records, columns=ENTITY_COLUMNS["customers"])
    frame.attrs["freq"] = freq
    frame.attrs["signup_ord"] = signup_ord
    return frame


def _build_promotions(
    rng: np.random.Generator, n: int, day_cume: np.ndarray, products: pd.DataFrame
) -> pd.DataFrame:
    pop_cume = _cume(products.attrs["popularity"])
    prod_idx = _search(pop_cume, rng.random(n))
    region_cume = _cume(np.asarray([w for _, _, w in REGIONS], dtype=np.float64))
    channel_cume = _cume(np.asarray([c[3] for c in CHANNELS], dtype=np.float64))
    region_flag = rng.random(n) < 0.70
    region_pick = _search(region_cume, rng.random(n))
    channel_flag = rng.random(n) < 0.80
    channel_pick = _search(channel_cume, rng.random(n))
    type_idx = rng.choice(len(_PROMO_TYPES), size=n, p=[t[3] for t in _PROMO_TYPES])
    discount_lo = np.asarray([t[1] for t in _PROMO_TYPES], dtype=np.float64)[type_idx]
    discount_hi = np.asarray([t[2] for t in _PROMO_TYPES], dtype=np.float64)[type_idx]
    discounts = np.round(rng.uniform(discount_lo, discount_hi), 4)
    windows = rng.integers(7, 22, size=n)

    product_ids = products["product_id"].to_numpy()
    records = []
    for i in range(n):
        max_start = PERIOD_DAYS - int(windows[i])
        sub = day_cume[: max_start + 1]
        start_ord = int(np.searchsorted(sub / sub[-1], rng.random(), side="right"))
        end_ord = start_ord + int(windows[i]) - 1
        records.append(
            {
                "promotion_id": f"PM{i + 1:04d}",
                "product_id": str(product_ids[int(prod_idx[i])]),
                "region_id": None if region_flag[i] else REGIONS[int(region_pick[i])][0],
                "channel_id": None if channel_flag[i] else CHANNELS[int(channel_pick[i])][0],
                "promotion_type": _PROMO_TYPES[int(type_idx[i])][0],
                "discount_pct": float(discounts[i]),
                "start_date": DAY_ISO[start_ord],
                "end_date": DAY_ISO[end_ord],
            }
        )
    return pd.DataFrame.from_records(records, columns=ENTITY_COLUMNS["promotions"])


@dataclass
class _World:
    """Reference arrays the sales and supply loops need, built once per generate()."""

    profile: ScaleProfile
    day_cume: np.ndarray
    hour_cume: tuple[np.ndarray, np.ndarray, np.ndarray]
    qty_cume: np.ndarray
    day_is_weekend: np.ndarray
    day_is_ramadan: np.ndarray
    channel_ids: np.ndarray
    channel_cume: np.ndarray
    store_ids: np.ndarray
    store_region: np.ndarray
    store_cume: np.ndarray
    product_ids: np.ndarray
    product_price: np.ndarray
    product_cost: np.ndarray
    product_margin: np.ndarray
    assort_prod: list[np.ndarray]
    assort_cume: list[np.ndarray]
    pair_index: np.ndarray
    pair_store: np.ndarray
    pair_product: np.ndarray
    promo_ids: np.ndarray
    promo_day_map: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]]
    cust_ids: np.ndarray
    cust_pref: np.ndarray
    glob_order: np.ndarray
    glob_signup: np.ndarray
    glob_cume: np.ndarray
    region_order: list[np.ndarray]
    region_signup: list[np.ndarray]
    region_cume: list[np.ndarray]


def _build_promo_day_map(
    promotions: pd.DataFrame,
    product_index: dict[str, int],
    region_index: dict[str, int],
    channel_index: dict[str, int],
) -> dict[int, tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]]:
    buckets: dict[int, list[tuple[int, int, int, int, float]]] = {}
    for row in promotions.itertuples(index=False):
        start = DATE_ORD[str(row.start_date)]
        end = DATE_ORD[str(row.end_date)]
        prod = product_index[str(row.product_id)]
        reg = -1 if row.region_id is None else region_index[str(row.region_id)]
        chan = -1 if row.channel_id is None else channel_index[str(row.channel_id)]
        promo_idx = int(str(row.promotion_id)[2:]) - 1
        for d in range(start, end + 1):
            buckets.setdefault(d, []).append(
                (prod, reg, chan, promo_idx, float(str(row.discount_pct)))
            )
    out: dict[int, tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray]] = {}
    for day, rows in buckets.items():
        arr = np.asarray(rows, dtype=np.float64)
        out[day] = (
            arr[:, 0].astype(np.int64),
            arr[:, 1].astype(np.int64),
            arr[:, 2].astype(np.int64),
            arr[:, 3].astype(np.int64),
            arr[:, 4],
        )
    return out


def _build_world(
    profile: ScaleProfile,
    rng: np.random.Generator,
    calendar: pd.DataFrame,
    day_cume: np.ndarray,
    stores: pd.DataFrame,
    products: pd.DataFrame,
    customers: pd.DataFrame,
    promotions: pd.DataFrame,
) -> _World:
    hour_cume = (
        _cume(np.asarray(_HOUR_WEEKDAY, dtype=np.float64)),
        _cume(np.asarray(_HOUR_WEEKEND, dtype=np.float64)),
        _cume(np.asarray(_HOUR_RAMADAN, dtype=np.float64)),
    )
    qty_cume = _cume(np.asarray(_QTY_WEIGHTS, dtype=np.float64))
    channel_ids = np.asarray([c[0] for c in CHANNELS])
    channel_cume = _cume(np.asarray([c[3] for c in CHANNELS], dtype=np.float64))

    n_stores = len(stores)
    region_index = {rid: i for i, (rid, _, _) in enumerate(REGIONS)}
    store_ids = stores["store_id"].to_numpy()
    store_region = stores["region_id"].map(region_index).to_numpy(dtype=np.int64)
    fmt_traffic = {f[0]: f[4] for f in _STORE_FORMATS}
    traffic = stores["store_format"].map(fmt_traffic).to_numpy(dtype=np.float64)
    region_total_weight = np.asarray([w for _, _, w in REGIONS], dtype=np.float64)
    n_per_region = np.bincount(store_region, minlength=len(REGIONS)).clip(min=1)
    region_share = region_total_weight[store_region] / n_per_region[store_region]
    raw_store_weight = traffic * rng.lognormal(0.0, 0.35, size=n_stores) * region_share
    raw_store_weight = np.where(stores["is_active"].to_numpy(dtype=bool), raw_store_weight, 0.0)
    if float(raw_store_weight.sum()) <= 0:
        raw_store_weight[0] = 1.0
    store_cume = _cume(raw_store_weight)

    product_ids = products["product_id"].to_numpy()
    product_price = products["unit_price_mad"].to_numpy(dtype=np.float64)
    product_cost = products["unit_cost_mad"].to_numpy(dtype=np.float64)
    product_margin = products["margin_pct"].to_numpy(dtype=np.float64)
    popularity = products.attrs["popularity"]
    pop_share = popularity / popularity.sum()

    assort_prod: list[np.ndarray] = []
    assort_cume_list: list[np.ndarray] = []
    pair_index = np.full((n_stores, len(products)), -1, dtype=np.int32)
    pair_store_list: list[int] = []
    pair_product_list: list[int] = []
    k = max(10, int(round(profile.assortment_ratio * len(products))))
    active_stores = stores["is_active"].to_numpy(dtype=bool)
    for s in range(n_stores):
        if active_stores[s]:
            chosen = rng.choice(len(products), size=k, replace=False, p=pop_share)
        else:
            chosen = np.empty(0, dtype=np.int64)
        assort_prod.append(chosen)
        if chosen.size:
            w = popularity[chosen]
            assort_cume_list.append(np.cumsum(w / w.sum()))
        else:
            assort_cume_list.append(np.empty(0, dtype=np.float64))
        for prod in chosen:
            pair_index[s, int(prod)] = len(pair_store_list)
            pair_store_list.append(s)
            pair_product_list.append(int(prod))
    assort_cume = assort_cume_list

    cust_ids = customers["customer_id"].to_numpy()
    cust_region = customers["region_id"].map(region_index).to_numpy(dtype=np.int64)
    cust_pref = (
        customers["preferred_channel_id"]
        .map({c[0]: i for i, c in enumerate(CHANNELS)})
        .to_numpy(dtype=np.int64)
    )
    freq = customers.attrs["freq"]
    freq = np.where(customers["is_active"].to_numpy(dtype=bool), freq, 0.0)
    signup_ord = customers.attrs["signup_ord"]

    glob_order = np.argsort(signup_ord, kind="stable")
    glob_signup = signup_ord[glob_order]
    glob_cume = np.cumsum(freq[glob_order])
    region_order: list[np.ndarray] = []
    region_signup: list[np.ndarray] = []
    region_cume: list[np.ndarray] = []
    for r in range(len(REGIONS)):
        idx = np.flatnonzero(cust_region == r)
        order_r = idx[np.argsort(signup_ord[idx], kind="stable")]
        region_order.append(order_r)
        region_signup.append(signup_ord[order_r])
        region_cume.append(np.cumsum(freq[order_r]))

    product_index = {pid: i for i, pid in enumerate(product_ids)}
    channel_index = {c[0]: i for i, c in enumerate(CHANNELS)}
    promo_day_map = _build_promo_day_map(promotions, product_index, region_index, channel_index)

    return _World(
        profile=profile,
        day_cume=day_cume,
        hour_cume=hour_cume,
        qty_cume=qty_cume,
        day_is_weekend=calendar["is_weekend"].to_numpy(dtype=bool),
        day_is_ramadan=calendar["is_ramadan"].to_numpy(dtype=bool),
        channel_ids=channel_ids,
        channel_cume=channel_cume,
        store_ids=store_ids,
        store_region=store_region,
        store_cume=store_cume,
        product_ids=product_ids,
        product_price=product_price,
        product_cost=product_cost,
        product_margin=product_margin,
        assort_prod=assort_prod,
        assort_cume=assort_cume,
        pair_index=pair_index,
        pair_store=np.asarray(pair_store_list, dtype=np.int64),
        pair_product=np.asarray(pair_product_list, dtype=np.int64),
        promo_ids=promotions["promotion_id"].to_numpy(),
        promo_day_map=promo_day_map,
        cust_ids=cust_ids,
        cust_pref=cust_pref,
        glob_order=glob_order,
        glob_signup=glob_signup,
        glob_cume=glob_cume,
        region_order=region_order,
        region_signup=region_signup,
        region_cume=region_cume,
    )


def _sample_customers(
    rng: np.random.Generator,
    world: _World,
    day_tx: np.ndarray,
    region_tx: np.ndarray,
) -> np.ndarray:
    """Sample one customer per transaction (or -1 for anonymous baskets)."""
    n_tx = day_tx.size
    eligible = rng.random(n_tx) >= _ANONYMOUS_RATE
    u_pool = rng.random(n_tx)
    k_glob = np.searchsorted(world.glob_signup, day_tx, side="right").clip(min=1)
    thr_glob = world.glob_cume[k_glob - 1]
    pos_glob = np.searchsorted(world.glob_cume, u_pool * thr_glob, side="right")
    pos_glob = np.minimum(pos_glob, world.glob_order.size - 1)
    cust = np.where(eligible & (thr_glob > 0), world.glob_order[pos_glob], -1)

    same_region = eligible & (rng.random(n_tx) < _SAME_REGION_RATE)
    for r in range(len(world.region_order)):
        order_r = world.region_order[r]
        if order_r.size == 0:
            continue
        rows = np.flatnonzero(same_region & (region_tx == r))
        if rows.size == 0:
            continue
        k_r = np.searchsorted(world.region_signup[r], day_tx[rows], side="right")
        thr_r = world.region_cume[r][np.maximum(k_r - 1, 0)]
        ok = (k_r > 0) & (thr_r > 0)
        if not ok.any():
            continue
        rows_ok = rows[ok]
        u_r = rng.random(int(rows_ok.size))
        pos_r = np.searchsorted(world.region_cume[r], u_r * thr_r[ok], side="right")
        cust[rows_ok] = order_r[pos_r]
    return cust


def _generate_sales_chunk(
    rng: np.random.Generator,
    world: _World,
    n_lines: int,
    sale_seq: int,
    tx_seq: int,
    demand: np.ndarray,
) -> tuple[pd.DataFrame, int]:
    sizes = _basket_sizes(rng, n_lines)
    n_tx = int(sizes.size)

    day_tx = _search(world.day_cume, rng.random(n_tx)).astype(np.int64)
    hour_u = rng.random(n_tx)
    hour_slot = np.where(
        world.day_is_ramadan[day_tx],
        _search(world.hour_cume[2], hour_u),
        np.where(
            world.day_is_weekend[day_tx],
            _search(world.hour_cume[1], hour_u),
            _search(world.hour_cume[0], hour_u),
        ),
    ).astype(np.int64)
    hour_tx = hour_slot + _HOURS[0]
    store_tx = _search(world.store_cume, rng.random(n_tx)).astype(np.int64)
    region_tx = world.store_region[store_tx]

    cust_tx = _sample_customers(rng, world, day_tx, region_tx)

    base_ch = _search(world.channel_cume, rng.random(n_tx)).astype(np.int64)
    pref_ch = world.cust_pref[np.clip(cust_tx, 0, None)]
    use_pref = (cust_tx >= 0) & (rng.random(n_tx) < _PREF_AFFINITY_RATE)
    ch_tx = np.where(use_pref, pref_ch, base_ch)

    line_tx = np.repeat(np.arange(n_tx, dtype=np.int64), sizes)
    line_store = store_tx[line_tx]
    line_day = day_tx[line_tx]

    order_s = np.argsort(line_store, kind="stable")
    uniq_s, starts_s, counts_s = np.unique(
        line_store[order_s], return_index=True, return_counts=True
    )
    line_prod = np.empty(n_lines, dtype=np.int64)
    for s_u, st, cnt in zip(uniq_s, starts_s, counts_s, strict=True):
        cum = world.assort_cume[int(s_u)]
        pos = np.searchsorted(cum, rng.random(int(cnt)) * cum[-1], side="right")
        line_prod[order_s[st : st + cnt]] = world.assort_prod[int(s_u)][pos]

    qty = rng.choice(np.asarray(_QTY_LEVELS), size=n_lines, p=np.asarray(_QTY_WEIGHTS)).astype(
        np.int64
    )

    promo_id_line = np.full(n_lines, None, dtype=object)
    eff_disc = np.zeros(n_lines, dtype=np.float64)
    line_region = world.store_region[line_store]
    line_ch = ch_tx[line_tx]
    order_d = np.argsort(line_day, kind="stable")
    uniq_d, starts_d, counts_d = np.unique(line_day[order_d], return_index=True, return_counts=True)
    for d_u, st, cnt in zip(uniq_d, starts_d, counts_d, strict=True):
        promo = world.promo_day_map.get(int(d_u))
        if promo is None:
            continue
        rows = order_d[st : st + cnt]
        prod_a, reg_a, chan_a, idx_a, disc_a = promo
        match = line_prod[rows][:, None] == prod_a[None, :]
        reg_ok = (reg_a[None, :] == -1) | (reg_a[None, :] == line_region[rows][:, None])
        ch_ok = (chan_a[None, :] == -1) | (chan_a[None, :] == line_ch[rows][:, None])
        elig = match & reg_ok & ch_ok
        any_elig = elig.any(axis=1)
        if not any_elig.any():
            continue
        first = np.argmax(elig, axis=1)
        exposed = any_elig & (rng.random(int(cnt)) < _PROMO_EXPOSURE)
        if not exposed.any():
            continue
        rows_exp = rows[exposed]
        sel = first[exposed]
        margin = world.product_margin[line_prod[rows_exp]]
        eff = np.round(np.minimum(disc_a[sel], margin * _MAX_DISCOUNT_OF_MARGIN), 4)
        eff_disc[rows_exp] = eff
        promo_id_line[rows_exp] = world.promo_ids[idx_a[sel]]
        lift = rng.random(int(exposed.sum())) < _PROMO_LIFT_PROB
        qty[rows_exp[lift]] += 1

    price = world.product_price[line_prod]
    cost_u = world.product_cost[line_prod]
    net = np.round(qty * price * (1.0 - eff_disc), 2)
    cost = np.round(qty * cost_u, 2)
    margin_mad = np.maximum(np.round(net - cost, 2), 0.0)

    cust_col = np.full(n_lines, None, dtype=object)
    cust_line = cust_tx[line_tx]
    has_cust = cust_line >= 0
    cust_col[has_cust] = world.cust_ids[cust_line[has_cust]]

    frame = pd.DataFrame(
        {
            "sale_id": np.char.mod("S%09d", sale_seq + np.arange(n_lines, dtype=np.int64)),
            "transaction_id": np.char.mod("T%09d", tx_seq + line_tx),
            "sale_date": DAY_ISO_ARR[line_day],
            "hour_of_day": hour_tx[line_tx],
            "store_id": world.store_ids[line_store],
            "channel_id": world.channel_ids[ch_tx[line_tx]],
            "customer_id": cust_col,
            "product_id": world.product_ids[line_prod],
            "promotion_id": promo_id_line,
            "quantity": qty,
            "unit_price_mad": price,
            "discount_pct": eff_disc,
            "net_amount_mad": net,
            "cost_amount_mad": cost,
            "margin_mad": margin_mad,
        },
        columns=ENTITY_COLUMNS["sales"],
    )

    pair = world.pair_index[line_store, line_prod]
    if (pair < 0).any():
        raise RuntimeError("sales line references a product outside the store assortment")
    n_windows = demand.shape[1]
    flat = pair.astype(np.int64) * n_windows + (line_day // world.profile.snapshot_days)
    demand += (
        np.bincount(flat, weights=qty, minlength=demand.size).astype(np.int64).reshape(demand.shape)
    )
    return frame, n_tx


def _simulate_supply(
    rng: np.random.Generator, world: _World, demand: np.ndarray
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Rolling order-up-to replenishment producing purchases + inventory snapshots."""
    cadence = world.profile.snapshot_days
    n_windows = demand.shape[1]
    pair_avg = demand.mean(axis=1)
    active = pair_avg > 0
    inv = np.where(active, np.maximum(1, np.ceil(0.5 * pair_avg)).astype(np.int64), np.int64(0))

    p_pairs: list[np.ndarray] = []
    p_qty: list[np.ndarray] = []
    p_win: list[np.ndarray] = []
    i_pairs: list[np.ndarray] = []
    i_qty: list[np.ndarray] = []
    i_status: list[np.ndarray] = []
    i_win: list[np.ndarray] = []

    for w in range(n_windows):
        if w == 0:
            hist_max = pair_avg
        else:
            hist_max = demand[:, max(0, w - _REPLENISH_HISTORY_WINDOWS) : w].max(axis=1)
        target = np.ceil(_REPLENISH_TARGET_FACTOR * hist_max).astype(np.int64)
        purch = np.maximum(target - inv, 0)
        sold = demand[:, w]
        inv = np.clip(inv + purch - sold, 0, None)

        sel_p = np.flatnonzero(purch > 0)
        if sel_p.size:
            p_pairs.append(sel_p)
            p_qty.append(purch[sel_p])
            p_win.append(np.full(sel_p.size, w, dtype=np.int64))

        keep = ~((inv == 0) & (sold == 0) & (purch == 0))
        sel_i = np.flatnonzero(keep)
        if sel_i.size:
            status = np.where(
                inv == 0,
                "out_of_stock",
                np.where(inv < 0.25 * np.maximum(target, 1), "low", "in_stock"),
            )
            i_pairs.append(sel_i)
            i_qty.append(inv[sel_i])
            i_status.append(status[sel_i].astype(str))
            i_win.append(np.full(sel_i.size, w, dtype=np.int64))

    if p_pairs:
        pairs = np.concatenate(p_pairs)
        qty = np.concatenate(p_qty)
        wins = np.concatenate(p_win)
        base_cost = world.product_cost[world.pair_product[pairs]]
        unit = np.maximum(np.round(base_cost * (1.0 + rng.normal(0.0, 0.012, qty.size)), 2), 0.01)
        total = np.round(qty.astype(np.float64) * unit, 2)
        purchases = pd.DataFrame(
            {
                "purchase_id": np.char.mod("PU%08d", np.arange(qty.size, dtype=np.int64)),
                "purchase_date": DAY_ISO_ARR[wins * cadence],
                "store_id": world.store_ids[world.pair_store[pairs]],
                "product_id": world.product_ids[world.pair_product[pairs]],
                "quantity": qty,
                "unit_cost_mad": unit,
                "total_cost_mad": total,
            },
            columns=ENTITY_COLUMNS["purchases"],
        )
    else:
        purchases = pd.DataFrame(columns=ENTITY_COLUMNS["purchases"])

    if i_pairs:
        pairs = np.concatenate(i_pairs)
        qty = np.concatenate(i_qty)
        wins = np.concatenate(i_win)
        status = np.concatenate(i_status)
        snapshot_idx = np.minimum((wins + 1) * cadence, PERIOD_DAYS) - 1
        value = np.round(qty.astype(np.float64) * world.product_cost[world.pair_product[pairs]], 2)
        inventory = pd.DataFrame(
            {
                "snapshot_date": DAY_ISO_ARR[snapshot_idx],
                "store_id": world.store_ids[world.pair_store[pairs]],
                "product_id": world.product_ids[world.pair_product[pairs]],
                "quantity_on_hand": qty,
                "stock_value_mad": value,
                "stock_status": status,
            },
            columns=ENTITY_COLUMNS["inventory"],
        )
    else:
        inventory = pd.DataFrame(columns=ENTITY_COLUMNS["inventory"])

    return purchases, inventory


def generate(seed: int, profile: ScaleProfile) -> dict[str, pd.DataFrame]:
    """Generate the full controlled synthetic dataset for ``profile`` and ``seed``.

    Returns one DataFrame per entity, ordered by ``schema.ENTITY_COLUMNS``.
    """
    rngs = spawn_rngs(seed)
    calendar = build_calendar()
    day_cume = _build_day_weights(calendar)
    regions, channels = _build_ref_frames()
    stores = _build_stores(rngs["stores"], profile.store_count)
    products = _build_products(rngs["products"], profile.product_count)
    customers = _build_customers(rngs["customers"], profile.customer_count)
    promotions = _build_promotions(rngs["promotions"], profile.promotion_count, day_cume, products)
    world = _build_world(
        profile, rngs["world"], calendar, day_cume, stores, products, customers, promotions
    )

    cadence = profile.snapshot_days
    n_windows = (PERIOD_DAYS + cadence - 1) // cadence
    demand = np.zeros((world.pair_store.size, n_windows), dtype=np.int64)
    n_chunks = (profile.sales_rows + profile.chunk_rows - 1) // profile.chunk_rows
    chunk_rngs = rngs["sales"].spawn(n_chunks)
    chunks: list[pd.DataFrame] = []
    sale_seq = 0
    tx_seq = 0
    for i in range(n_chunks):
        start = i * profile.chunk_rows
        stop = min(start + profile.chunk_rows, profile.sales_rows)
        frame, n_tx = _generate_sales_chunk(
            chunk_rngs[i], world, stop - start, sale_seq, tx_seq, demand
        )
        chunks.append(frame)
        sale_seq += stop - start
        tx_seq += n_tx
    sales = pd.concat(chunks, ignore_index=True) if len(chunks) > 1 else chunks[0]

    purchases, inventory = _simulate_supply(rngs["purchases"], world, demand)

    tables: dict[str, pd.DataFrame] = {
        "regions": regions,
        "channels": channels,
        "stores": stores,
        "products": products,
        "customers": customers,
        "promotions": promotions,
        "sales": sales,
        "purchases": purchases,
        "inventory": inventory,
        "calendar": calendar,
    }
    for entity, frame in tables.items():
        tables[entity] = frame[list(ENTITY_COLUMNS[entity])]
        # Internal helper arrays (popularity, signup order) were consumed by
        # _build_world; strip attrs so writers (notably Parquet) never try to
        # serialize DataFrame.attrs as file metadata.
        tables[entity].attrs.clear()

    if len(sales) != profile.sales_rows:
        raise RuntimeError(
            f"sales row count mismatch: expected {profile.sales_rows}, got {len(sales)}"
        )
    return tables
