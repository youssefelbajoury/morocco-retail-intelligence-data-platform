"""Deterministic controlled synthetic data generation (PHASE 02).

See ``docs/data-model/synthetic-data.md`` for entities, relationships,
generation rules, scale profiles, seed semantics and documented limitations.
"""

from mri.synthetic.generator import generate
from mri.synthetic.profiles import PROFILE_NAMES, SCALE_PROFILES, ScaleProfile, get_profile

__all__ = ["generate", "PROFILE_NAMES", "SCALE_PROFILES", "ScaleProfile", "get_profile"]
