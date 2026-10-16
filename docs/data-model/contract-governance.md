# Data model governance — contract drift detection

> **Status: IMPLEMENTED (Phase 15).** The source contracts in `data_contracts/`
> are the single source of truth for what every source must look like. Their
> runtime is implemented in three places that can silently drift out of sync:
> the Great Expectations bronze gate
> (`src/mri/ingestion/quality.py`), Bronze standardization
> (`src/mri/ingestion/bronze_schema.py`) and the Silver DDL/loader
> (`src/mri/silver/ddl.py`). The drift detector
> (`src/mri/governance/drift.py`, exercised by
> `tests/unit/test_contract_drift.py`) proves, in CI, that the three do not
> diverge — while understanding the documented abstractions so it never raises
> a false alarm. Decision record:
> [`ADR-012`](../decisions/ADR-012-contract-drift-detection.md).

---

## 1. Why contracts drift

A `quality.declared` rule is a **natural-language promise**. Making that
promise real requires mirrored knowledge in static code:

| Contract concern | Implementation |
|------------------|----------------|
| `quality.declared` range/set/pair/regex lines | `quality.py:_declared_specs` (one hand-written `add(...)` per source) |
| `X exists in Y` referential rules | `quality.py:REFERENCE_COLUMNS` (source → dimension/FK column pairs) |
| Volume, not-null, types, key uniqueness | `quality.py:_generic_specs` (derived from the contract itself — cannot drift) |
| Bronze partitioning by date | `bronze_schema.py:PARTITION_COLUMNS` (which source is cut by which column) |
| Source → Silver table mapping | `silver/ddl.py:SOURCE_TABLES` (source id → `raw_staging`/`silver` table) |
| Column renderability + reserved names | `silver/ddl.py:PG_TYPES`, `PROVENANCE_COLUMNS` (appended to every Silver table) |

An engineer who edits one of those places without the others quietly breaks
the contract. The detector closes that gap in both directions: nothing may be
declared without being implemented, and nothing may be implemented without
being declared (referentially it is the models that own the rule, so the
static runtime tables carry the "implemented" side).

## 2. The canonical rule model

The detector turns both the contract text and the runtime expectations into
the **same** canonical shape, so comparison is semantic, never textual:

| Kind | Contract line | Runtime expectation |
|------|---------------|---------------------|
| `uniqueness` | `sale_id is unique and not null`; `date, base and quote are unique and not null`; `composite key (snapshot_date, store_id, product_id) is unique` | generic primary-key uniqueness + not-null |
| `referential` | `store_id exists in stores` | `REFERENCE_COLUMNS` → `ExpectColumnValuesToBeInSet` |
| `between` | `amount between 0 and 100`; `rate > 0`; `launch_date <= 2024-12-31`; `net_amount_mad >= 0 and margin_mad >= 0` | `ExpectColumnValuesToBeBetween` |
| `set` | `channel_type in {physical, digital}` | `ExpectColumnValuesToBeInSet` |
| `regex` | `external_product_id matches ^[0-9]{8,14}$` | `ExpectColumnValuesToMatchRegex` |
| `pair` | `end_date >= start_date`; `unit_price_mad >= unit_cost_mad` | `ExpectColumnPairValuesAToBeGreaterThanB` |

One declared line may map to several canonical rules (`net >= 0 and margin >=
0` → two `between` rules), and the detector tolerates grammar variants
(`is between` / `between`, `is/are unique and not null`, `composite key (...)`).

## 3. The five drift classes

`run_drift_report()` returns a `DriftReport` whose `issues` make the build
fail; `warnings` are acknowledged exceptions.

| Class | Example | Failure |
|-------|---------|---------|
| **declared but not implemented** | a contract promises `amount between 0 and 100` but `_declared_specs` adds nothing for it | `declared_not_implemented` |
| **implemented but not declared** | `_declared_specs` / `REFERENCE_COLUMNS` enforce a rule no contract line publishes | `implemented_not_declared` |
| **source mapping missing** | a new contract id is absent from `SOURCE_TABLES` (or a map entry has no contract file) | `source_mapping_missing` / `source_mapping_orphan` |
| **contract version mismatch** | non-semver, a major other than `1`, or the same id declared by two files | `version_mismatch` |
| **unsupported column** | a column named like Silver provenance (`batch_id`, `source`, …), an unrenderable type, or a missing Bronze partition column | `unsupported_column` |

The **uniqueness** declared lines are checked against the contract's own
`keys.primary` and nullability: the generic expectations derive from those, so
the detector verifies the declaration agrees with the contract frame rather
than duplicating the code (`sale_id is unique and not null` is satisfied
exactly when `primary = [sale_id]` and `sale_id` is non-nullable).

## 4. Documented exceptions (no false positives)

The detector is deliberately conservative where the architecture abstracts
implementation details:

1. **Generic-rule coverage.** Declared uniqueness/not-null lines are enforced
   by contract-derived generic expectations, so they never need a literal
   presence in `_declared_specs`.
2. **Grammar tolerance.** Natural-language variants parse onto the same
   canonical rule (`is between`, compound `and`, `are unique`, `composite key`,
   sorted set membership, `\d` ≙ `[0-9]` in regexes).
3. **Referential rules are resolved statically** through `REFERENCE_COLUMNS`,
   because whether the set expectation materializes depends on runtime
   reference data. Drift there is a code-to-code comparison, not a live run.
4. **Bronze-only sources.** A source intentionally kept out of the Silver map
   is declared in `BRONZE_ONLY_SOURCES` (currently empty — all 14 sources land
   in Silver) and only produces a warning. Add future enrichment-only sources
   to that set instead of deleting contracts.
5. **Unclassifiable lines are warnings, not failures.** A declared line the
   parser does not understand surfaces as a warning; CI fails only on real,
   reproducible drift.
6. **Partial-directory probes.** When probing a contracts subdirectory the
   caller should scope `source_tables` to what the directory declares;
   otherwise unrelated `SOURCE_TABLES` entries look like orphans (see the
   tests, which pass an explicit map for temp directories).

## 5. Running the detector

```bash
# The gate (True / False) against the repo contracts:
uv run python -c "from mri.governance.drift import check; print(check())"
# A full machine-readable report:
uv run python -c "from mri.governance.drift import run_drift_report; r = run_drift_report(); print(r)"
```

CI runs `tests/unit/test_contract_drift.py` as part of the `unit` job
(`pytest tests/unit`), so any of the five drift classes breaks a push/PR.
Current probe of the real contract directory (Phase 15):

```
sources_checked=14  declared_rules=84  implemented_rules=59  issues=0  warnings=0
```

## 6. Contract-first change procedure

For any change to a source shape or its quality:

1. **Edit the contract** in `data_contracts/<source>.yml` (and bump the
   version — MAJOR for breaking key/column/type changes, per the README).
2. **Implement it** in the layer that owns the rule (`quality.py`,
   `bronze_schema.py`, `silver/ddl.py`).
3. **Run the drift test** (`uv run pytest tests/unit/test_contract_drift.py -q`).
   A green report is the proof that the three layers still agree.
4. Update `docs/data-model/source-contracts.md` and this page when the change
   adds a source or a new rule idiom.