# ADR-012 — Contract drift detection (contracts as the single source of truth)

- **Status:** Accepted
- **Date:** 2026-10-09
- **Phase:** 15 — Data contract drift detection
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

The versioned YAML contracts in `data_contracts/` are the platform's agreement
with every producer, but the **implementation** of that agreement is spread
across hand-written Python:

- `src/mri/ingestion/quality.py:_declared_specs` maps each contract's
  `quality.declared` natural-language lines onto Great Expectations
  expectations, one `add(...)` per source;
- `quality.py:REFERENCE_COLUMNS` encodes referential rules statically;
- `src/mri/ingestion/bronze_schema.py:PARTITION_COLUMNS` decides which sources
  are date-partitioned;
- `src/mri/silver/ddl.py:SOURCE_TABLES`, `PG_TYPES` and `PROVENANCE_COLUMNS`
  define how a contract becomes a Silver table.

Each of these is duplicated knowledge. Nothing connected the contract text to
the runtime expectations; a rule could be added in YAML and never enforced, or
added to `_declared_specs` and never published in YAML, and CI would stay
green. The phase had to decide how to make the contract authoritative without
redesigning the pipeline or screaming false alarms.

## Decision

1. **Add a drift detector (`src/mri/governance/drift.py`) that proves, in CI,
   that the contracts and the runtime agree in both directions.** A
   canonical rule model (`uniqueness`, `referential`, `between`, `set`,
   `regex`, `pair`) is produced from both the contract text and the real
   expectations that `quality.build_specs` generates (introspected in memory
   on a skeleton frame — no GX execution, no database). Every declared rule
   must have an implemented counterpart and vice-versa; referential rules are
   compared statically against `REFERENCE_COLUMNS`.
2. **Detect the five drift classes the phase names:** declared-but-not-
   implemented, implemented-but-not-declared, source mapping missing, contract
   version mismatch, unsupported column (reserved provenance names) — plus
   orphaned `SOURCE_TABLES` entries and a missing Bronze partition column.
3. **Honour the documented exceptions so the detector never false-positives:**
   declared uniqueness/not-null lines are enforced by **generic** (contract-
   derived) expectations, so they are checked against the contract's own
   primary key and nullability; grammar variants (`is between`, compound
   `X >= a and Y >= b`, `are/is unique and not null`, `composite key (...)`,
   sorted sets, `\d` vs `[0-9]`) are normalized; bronze-only sources are a
   declared exception
   (`BRONZE_ONLY_SOURCES`, currently empty) that warns instead of failing; a
   line the parser cannot classify is a warning, never a failure.
4. **No production code is rewritten.** The detector is additive and reads the
   existing implementation; the contract-loading and drift checks stay in the
   unit job (`tests/unit/test_contract_drift.py`), which the `unit` CI job
   already runs.
5. **The contract stays the first-class artifact.** A rule an engineer adds to
   a contract without implementing it fails the build immediately; a rule
   added to the code without a contract line fails the build immediately.

## Why not the alternatives

| Alternative | Why not chosen |
|-------------|----------------|
| A structured rule grammar in YAML that `quality.py` interprets directly | The most robust (single source), but a rewrite of `_declared_specs` and all 14 contracts with real risk of silently changing gate semantics; Phase 15's mandate is detection + governance, not re-architecting the gate. The detector still *proves* the contract is the source of truth. |
| Keep `quality.declared` strings and diff them textually against the code's strings | Fragile: the same semantic rule is spelled differently (`is between`, compound `and`, sorted sets, regex `\d{8,14}` vs `[0-9]{8,14}`), so a textual diff is a false-positive factory. |
| Runtime check only at ingestion (`silver/ddl.py` already raises on unknown sources) | Only fires when someone runs a load against a live store; it never catches a rule a contract declares but nobody enforces, and gives no reviewable report. |
| A one-off audit script, not part of CI | Drift is about *prevention*; the phase requires the check to be in the test suite so a merge can never introduce silent divergence. |
| Detect only contract→implementation (single direction) | Half of drift is code that enforces rules the contract never published ("stricter than promised"); both directions are required for the contract to remain truthful. |

## Consequences

- **Positive:** a merge that desynchronizes any of the three layers breaks CI
  with a named drift class and the offending contract line; the detector is a
  pure, additive, offline check (fast — the whole repo probe is a few seconds
  in the unit suite); it validates the *current* contracts too, so the report
  doubles as a living audit (14 sources, 84 declared rules, 59 implemented
  expectations, zero drift as of Phase 15); `BRONZE_ONLY_SOURCES` gives the
  architecture a place to document enrichment-only sources instead of
  papering them over.
- **Negative:** the canonical parser is a new, small vocabulary of rule idioms —
  a genuinely novel rule shape must first be taught to the classifier or it is
  (safely) reported as an unclassifiable warning; regex equivalence is token
  normalization, not full PCRE semantics (belt, not suspenders); probing a
  contracts *subdirectory* requires the caller to scope `source_tables`, since
  the orphan pass sees the whole catalog.
- **Follow-ups:** if a third enforcement layer appears, extend the implemented
  side of the detector; consider teaching the classifier additional idioms as
  the contract suite grows; keep the probe metrics under §5 of
  `docs/data-model/contract-governance.md` fresh.

---

## Verification record

- `src/mri/governance/drift.py` — canonical rule model, both direction passes,
  source-map / version / column checks, documented exceptions.
- `tests/unit/test_contract_drift.py` — 15 tests: happy path on the real
  `data_contracts/` (0 issues, 0 warnings) plus one negative path per drift
  class and the bronze-only exception. Entire unit suite green (334 tests).
- `ruff check`, `ruff format --check`, `mypy` — clean.
- Probe snapshot: `sources_checked=14  declared_rules=84  implemented_rules=59
  issues=0  warnings=0`.