# Data Quality Gate (Phases 06–07)

> **Status: IMPLEMENTED (Phase 06, extended Phase 07).** Every Bronze batch is
> validated with **Great Expectations 1.23** before it can be promoted — both
> the ten internal `raw/` sources and the four external `raw_external/`
> sources (whose declared rules joined the suite in Phase 07). The gate is a
> *decision*, not a report: a failed batch keeps its parts, its quality report
> and (when rows were excluded) a rejected-rows artifact — but never a
> promotion marker.

---

## 1. Philosophy

1. **Quality is a gate.** Downstream layers only read batches listed under
   `_promoted/`. Nothing is "loaded anyway with warnings".
2. **A batch fails as a whole.** One bad row or one failed expectation blocks
   the entire batch — Bronze batches are per-raw-object and therefore small.
3. **Failure never destroys evidence.** RAW is untouched, Parquet parts are
   kept for debugging, the JSON report states exactly what failed, and excluded
   rows are written to `_rejected/`.
4. **Two independent verdicts.** GX expectations *and* standardization
   rejections both gate promotion: a batch where GX passes but 3 rows were
   dropped during typing is still `failed` (`N row(s) rejected during
   standardization`), because the Parquet no longer equals the raw data.

---

## 2. Suite composition

Suites are built per batch in `src/mri/ingestion/quality.py` from three
layers:

### 2.1 Generic contract rules (every source)

| Rule | GX expectation | Source of truth |
|------|----------------|-----------------|
| Row count within bounds | `ExpectTableRowCountToBeBetween` | `raw.volume.min_rows/max_rows` |
| Non-nullable columns | `ExpectColumnValuesToNotBeNull` | contract `nullable: false` |
| Column types | `ExpectColumnValuesToBeInTypeList` | contract type → numpy dtype |
| Primary-key uniqueness | `ExpectColumnValuesToBeUnique` (single) / `ExpectCompoundColumnsToBeUnique` (composite) | contract `primary_key` |

### 2.2 Declared quality rules (`quality.declared` → concrete expectations)

Each contract ships natural-language rules; `_declared_specs` maps them 1:1:

| Source | Declared rule | Expectation |
|--------|---------------|-------------|
| calendar | month 1–12 · quarter 1–4 | `…ToBeBetween` |
| channels | type ∈ {physical, digital} · demand_weight 0–1 | `…InSet` / `…Between` |
| customers | birth_year 1900–2024 · gender ∈ {F,M,U} · tier ∈ {bronze,silver,gold,platinum} | `…InSet` / `…Between` |
| frankfurter_fx | date 1999–2100 · base ∈ {EUR} · quote ∈ {GBP,MAD,USD} · rate > 0 | `…Between` / `…InSet` |
| inventory | qty ≥ 0 · stock_value ≥ 0 · status ∈ {in_stock,low,out_of_stock} | `…Between` / `…InSet` |
| open_meteo_forecast | forecast_date 1940–2100 · lat/lon bounds · temps −80–60 · tmax ≥ tmin · precip ≥ 0 · hours 0–24 · wind ≥ 0 · code 0–99 | `…Between` / pair `A ≥ B` (null-skipping) |
| open_meteo_history | observation_date 1940–2100 · (same measure rules as forecast) | `…Between` / pair `A ≥ B` (null-skipping) |
| openfoodfacts_products | barcode `^[0-9]{8,14}$` · mapping_status ∈ {MATCHED,NEW,UNMATCHED} · nova 1–4 · energy 0–900 · fat/sugars/proteins/salt 0–100 | `…MatchRegex` / `…InSet` / `…Between` |
| products | unit_cost > 0 · price ≥ cost · margin_pct 0–1 · launch_date ≤ 2024-12-31 | `…Between` / pair `A ≥ B` |
| promotions | discount_pct 0–1 · end_date ≥ start_date | `…Between` / pair `A ≥ B` |
| purchases | quantity ≥ 1 · unit_cost > 0 · total_cost ≥ 0 | `…Between` |
| regions | demand_weight 0–1 | `…Between` |
| sales | quantity ≥ 1 · hour 8–23 · discount 0–1 · net_amount ≥ 0 · margin ≥ 0 | `…Between` |
| stores | size_sqm > 0 · open_date ≤ 2024-12-31 | `…Between` |

External rows (Phase 07) are documented in
[`external-bronze.md`](external-bronze.md); the nullable weather pair runs
with `ignore_row_if="either_value_is_missing"` so an incomplete day skips the
pair instead of failing the batch.

### 2.3 Referential checks (cross-source)

| Source | Column | Must exist in |
|--------|--------|---------------|
| customers | `region_id` · `preferred_channel_id` | regions · channels |
| open_meteo_forecast | `region_id` | regions |
| open_meteo_history | `region_id` | regions |
| purchases | `store_id` · `product_id` | stores · products |
| promotions | `product_id` | products |
| sales | `store_id` · `product_id` · `channel_id` | stores · products · channels |
| stores | `region_id` | regions |

Implemented as `ExpectColumnValuesToBeInSet` against the primary keys read
from the dimension's raw objects. If a dimension has no raw objects yet, the
check is **skipped with a note** in `skipped_expectations` (Bronze is built per
source; ordering is the caller's responsibility) — a skip is visible in the
report, never silent.

---

## 3. Report

One JSON per batch at `bronze/_quality/<run_id>/<source>/<batch>.json`:

```json
{
  "run_id": "20261007T115146Z-8f3a1c",
  "source": "sales",
  "batch": "sales__0123456789abcdef",
  "status": "passed",
  "expectations_total": 38,
  "expectations_passed": 38,
  "expectations_failed": 0,
  "records_checked": 12500,
  "records_rejected": 0,
  "contract": {"id": "sales", "version": "1.0.0"},
  "reason": null,
  "failed_expectations": [],
  "skipped_expectations": [],
  "started_at": "…", "finished_at": "…",
  "promoted": true
}
```

Required fields per spec (`run_id, source, batch, status,
expectations_total, expectations_passed, expectations_failed,
records_checked`) are always present; extras (`records_rejected`, contract
identity, `reason`, failed/skipped details, timestamps, `promoted`) make the
report self-contained for audit. `failed_expectations` lists at most
`MAX_FAILED_DETAILS = 20` entries, each with the GX expectation type, the
contract rule it enforces (`declared`), `unexpected_count` and
`observed_value`.

**Status semantics**

| `status` | Condition |
|----------|-----------|
| `passed` | `expectations_failed == 0` **and** `records_rejected == 0` |
| `failed` | any GX failure, any rejected row, or a fatal parse (reason set, 0 expectations) |

---

## 4. GX implementation notes (hard-won details)

* **Ephemeral context per run** (`gx.get_context(mode="ephemeral")`) — no
  checkpoint files, no `gx/` state directory, fully idempotent; progress bars
  disabled via `ProgressBarsConfig(globally=False)` so the CLI stays clean.
* **Result order ≠ suite order.** `run.results` is not aligned with the
  suite's expectation list, so failures are matched back to the contract rule
  through a `_match_key(expectation_type, kwargs)` lookup (`column`,
  `column_A` or `column_list`), not by index.
* **Datetime bounds must be `datetime`, not `date`.** GX's pydantic model
  coerces a date-only bound (`'2024-12-31'`) to `datetime.date`, and pandas
  cannot compare `datetime.date` against `datetime64[ns]` — the expectation
  explodes into `success: False` with an empty `result` (misleadingly looking
  like a real failure). Passing `datetime(2024, 12, 31)` avoids the coercion.
* **Type expectations only for numpy dtypes.** Nullable columns use pandas
  extension dtypes (`Int64`, `Float64`, `boolean`), which GX's type list does
  not recognise; those expectations are deliberately not emitted (documented
  in `_TYPE_LISTS`). Types are still *enforced* — by the strict standardizer.
* **`…ToBeInSet` counts null as unexpected.** All FK columns are
  non-nullable, so this never fires spuriously; noted for future nullable
  set-checks.
* **Empty frames**: a batch of 0 rows still evaluates (volume expectation
  typically fails → batch blocked).

---

## 5. Artifacts on failure

| Artifact | When | Purpose |
|----------|------|---------|
| `bronze/_quality/.../<batch>.json` | always | what exactly failed |
| `bronze/_rejected/<source>/<batch>.json` | `records_rejected >` 0 | excluded rows (≤100 + per-code counts) |
| Parquet parts | non-fatal failures | debugging / reprocessing |
| `bronze/_manifests/<run_id>.json` + `reports/bronze/<run_id>.json` | always | run outcome, `failed` counts |
| `_promoted/...` | **never** on failure | — |

CLI exit code `1` whenever any batch failed.

---

## 6. Tests

`tests/unit/test_quality.py` covers: passing batch; negative quantity;
out-of-range discount; duplicate primary keys (`unexpected_count`); a batch
where GX passes but conversion rejected rows (must fail); referential checks
skipped when the dimension is empty and failing when a value is unknown;
`failed_quality` shape; `with_promotion`. Pipeline-level behaviour (no
promotion marker, parts retained, rejected artifact) lives in
`tests/unit/test_bronze_pipeline.py` and `tests/integration/test_bronze_minio.py`.

---

See also: [`bronze-layer.md`](bronze-layer.md) ·
[`ADR-004`](../decisions/ADR-004-bronze-parquet-and-quality-gate.md) ·
[`data_contracts/README.md`](../../data_contracts/README.md)
