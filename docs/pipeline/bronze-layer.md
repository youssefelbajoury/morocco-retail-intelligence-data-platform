# The Bronze Layer (Phase 06)

> **Status: IMPLEMENTED (Phase 06, extended Phase 07).**
> `src/mri/ingestion/bronze_schema.py`, `src/mri/ingestion/quality.py`,
> `src/mri/ingestion/bronze.py` + `apps.ingestion bronze` convert
> contract-governed RAW objects in MinIO into partitioned, schema-standardized
> Parquet with provenance, gated by a Great Expectations suite. Failed data is
> retained and reported, never promoted and never discarded. Phase 07 extended
> the same pipeline to the verbatim `raw_external/` responses (normalization,
> four external contracts, sidecar gating) — see
> [`external-bronze.md`](external-bronze.md).

---

## 1. What it does

```mermaid
flowchart TD
    R["retail-raw<br/>raw/&lt;source&gt;/&lt;name&gt;__&lt;hash16&gt;.csv"] --> DL["download bytes<br/>sha256 = source_hash"]
    DL --> P["parse_rows<br/>CSV / JSON → all-text frame"]
    P -->|fatal (missing column / bad format)| QF["quality report: failed<br/>no parts written"]
    P --> S["standardize<br/>typed columns, strict conversion"]
    S -->|invalid rows| RJ["rows excluded<br/>rejected/_rejected/&lt;source&gt;/&lt;batch&gt;.json"]
    S --> PA["build_parts<br/>partitioned Parquet + provenance"]
    PA --> GX["Great Expectations suite<br/>volume · types · nulls · keys · declared rules · FK"]
    GX -->|passed| MK["_promoted/&lt;source&gt;/&lt;batch&gt;.json<br/>+ quality report + manifest"]
    GX -->|failed| FQ["parts kept for debugging<br/>quality report, NO promotion marker"]
    RJ --> GX
```

For every raw object the pipeline:

1. **Downloads** the bytes and computes `sha256` — the provenance hash is over
   the exact raw bytes (no object-stat API needed).
2. **Parses** CSV (stdlib `csv`, same header semantics as Phase-04: stripped
   names, later duplicate wins, ragged rows padded with `""`) or JSON
   (objects flattened to text-or-`None` cells) into an all-text frame.
3. **Standardizes** every contract column with strict, vectorised conversion —
   an unparsable value never silently coerces (`"not-a-number"` never becomes
   `0`); violating rows are excluded and reported.
4. **Writes partitioned Parquet parts** with five provenance columns.
5. **Runs the quality gate** (Great Expectations) over the standardized frame,
   combining generic contract rules, the contract's declared quality rules and
   cross-source referential checks.
6. **Promotes** only when the gate passes *and* no row was rejected.

---

## 2. Object layout

```
bucket: retail-raw                                   (read-only for Bronze)
└── raw/<source>/<name>__<hash16>.csv                immutable input

bucket: retail-bronze
├── bronze/<source>/part-<batch>.parquet                     unpartitioned sources
├── bronze/sales/year=YYYY/month=MM/part-<batch>.parquet     partitioned facts
├── bronze/purchases/year=YYYY/month=MM/part-<batch>.parquet
├── bronze/inventory/year=YYYY/month=MM/part-<batch>.parquet
├── bronze/_quality/<run_id>/<source>/<batch>.json      machine-readable report
├── bronze/_promoted/<source>/<batch>.json              promotion marker (pass only)
├── bronze/_rejected/<source>/<batch>.json              excluded rows (when any)
└── bronze/_manifests/<run_id>.json                     audit trail per run

local: reports/bronze/<run_id>.json                     same manifest, local copy
```

* **Batch id** = raw filename stem (`sales__<hash16>`) — content-derived, so the
  same raw object always maps to the same part keys (idempotent reruns).
* **Part name** = `part-<batch>.parquet`, filesystem-sanitised (no `/`, no
  spaces, ≤180 chars).
* **Partitioning follows the access pattern**: only the large fact sources
  (`sales`, `purchases`, `inventory`) are split by `year=/month=/` on their
  date column; everything else is one object per batch. A `NaT` partition would
  land in `year=__unknown__/month=__unknown__` (defensive — contracts make the
  date non-nullable, so it is unreachable).

---

## 3. Provenance

Every part carries five extra columns appended after the contract columns:

| Column | Value |
|--------|-------|
| `source` | contract source id (`sales`, …) |
| `source_file` | raw object key |
| `source_hash` | `sha256` of the raw bytes |
| `ingestion_run_id` | Bronze run id |
| `ingestion_timestamp` | ISO-8601 stamp of the batch conversion |

---

## 4. Standardization rules

Conversion is per contract type; **nulls** are `None`/empty/whitespace-only
cells:

| Contract type | Accepted | Rejected (row excluded) | Result dtype (no nulls / with nulls) |
|---------------|----------|--------------------------|--------------------------------------|
| `string` | any text (kept verbatim) | — | `object` (`None` for nulls) |
| `integer` | `[+-]?[0-9]+`, \|v\| < 2⁶³ | `"12.0"`, `"12.5"`, `"1e3"` | `int64` / `Int64` |
| `number` | finite floats | `NaN`, `inf`, unparsable | `float64` / `Float64` |
| `boolean` | `true/1/false/0` (any case) | `yes`, `maybe` | `bool` / `boolean` |
| `date` | `YYYY-MM-DD` (strict) | other formats | `datetime64[ns]` |

* Null in a **non-nullable** column → row excluded (`null_violation`);
  null in a **nullable** column → kept as `None`.
* A missing **required column** is *fatal*: the batch fails with a quality
  report and **no parts** are written.
* Extra columns outside the contract are preserved after the contract columns.
* Caps: at most `MAX_ISSUES = 1000` individual issues are recorded (counts stay
  exact) and `MAX_REJECTED_ROWS = 100` rejected rows are dumped per batch.

Issue codes: `null_violation`, `invalid_integer`, `invalid_number`,
`invalid_boolean`, `invalid_date` (mirroring the contract's `quality.declared`
vocabulary).

---

## 5. Failure handling (never promote, never discard)

| Situation | Parquet parts | Quality report | Rejected artifact | Promotion marker |
|-----------|---------------|----------------|-------------------|------------------|
| All rows valid, GX passes | written | `status: passed` | — | `_promoted/…` ✅ |
| GX rule fails | written (kept for debugging) | `status: failed` + failed expectations | if rows were rejected | **absent** |
| Conversion rejected rows | written | `status: failed` (`N row(s) rejected`) | `_rejected/…` | **absent** |
| Fatal (missing column / bad format) | none | `status: failed` + reason | — | **absent** |
| `--dry-run` | none | none | none | none |

RAW objects are never modified — Bronze is a pure reader of Raw (integration
tests compare raw bytes before/after). Downstream layers must only consume
batches listed under `_promoted/`.

---

## 6. CLI

```bash
python -m apps.ingestion bronze --source all                    # every raw batch
python -m apps.ingestion bronze --source sales --source regions
python -m apps.ingestion bronze --batch raw/sales/sales__0123.csv
python -m apps.ingestion --json bronze --source sales --dry-run # plan, no GX, no writes
```

Exit codes: `0` all batches passed/planned, `1` at least one batch failed,
`2` usage error (unknown source, `all` combined with another `--source`,
explicit source/batch with no raw objects).

`--dry-run` still reads Raw (to report what would happen) but writes nothing
and skips Great Expectations entirely.

---

## 7. Idempotence

Part keys are derived from the raw object's content hash, so a rerun maps to
the same keys:

* existing parts are skipped → outcome `already_bronzed`;
* the quality suite is **re-evaluated every run** (quality is a property of the
  data, not of the write);
* promotion markers are rewritten with the new `run_id`;
* a previously promoted batch whose re-validation now *fails* simply gets no new
  marker for that run — consumers always read the latest state through
  `_promoted/` manifests.

---

## 8. Benchmarks

`benchmarks/bronze_benchmark.py` (results appended to
`benchmarks/bronze/results.json`):

```bash
uv run python benchmarks/bronze_benchmark.py --scale small
uv run python benchmarks/bronze_benchmark.py --scale medium --skip-ingest
```

Measured on the development host (10 batches, all quality rules, snappy
compression):

| Scale | Rows in | Input → Output | Elapsed | Throughput | Quality |
|-------|---------|----------------|---------|------------|---------|
| small | 126 348 | 8.8 MB → 3.8 MB | 22.2 s | ~5 700 rows/s | 10/10 passed |
| medium | 1 397 019 | 92.7 MB → 23.6 MB | 134.0 s | ~10 400 rows/s | 10/10 passed |

---

## 9. Tests

* `tests/unit/bronze_fixtures.py` — contract-shaped valid/invalid batches.
* `tests/unit/test_bronze.py` — parsing, strict conversion, rejection codes,
  fatal handling, caps, partition keys, determinism, provenance.
* `tests/unit/test_quality.py` — every gate semantic (pass, declared-rule
  failures, duplicate keys, conversion-rejection, referential skip/fail,
  report shape).
* `tests/unit/test_bronze_pipeline.py` — promotion, failure handling,
  idempotence, dry-run, usage errors, manifests (in-memory object store).
* `tests/unit/test_bronze_cli.py` — exit codes and summaries (offline).
* `tests/integration/test_bronze_minio.py` — live MinIO: raw untouched
  byte-for-byte, parquet readable, no promotion on failure, idempotent rerun.

---

See also: [`data-quality.md`](data-quality.md) ·
[`external-bronze.md`](external-bronze.md) ·
[`silver-layer.md`](silver-layer.md) ·
[`ADR-004`](../decisions/ADR-004-bronze-parquet-and-quality-gate.md) ·
[`ADR-005`](../decisions/ADR-005-external-bronze-contracts.md) ·
[`file-ingestion.md`](file-ingestion.md)
