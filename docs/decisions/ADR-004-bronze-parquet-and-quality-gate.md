# ADR-004 — Partitioned Parquet Bronze with a Great Expectations gate

- **Status:** Accepted
- **Date:** 2026-10-07
- **Phase:** 06 — Bronze layer (RAW → typed Parquet + quality gate)
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phases 02–05 delivered a deterministic synthetic retailer, a local container
stack, versioned source contracts and two raw ingestion paths (files under
`raw/`, verbatim API responses under `raw_external/`). RAW is immutable and
all-text by design — but nothing downstream can query it yet: no columnar
format, no types, no enforced quality, no promotion semantics.

Before building Silver in PostgreSQL, four questions had to be settled:

1. **Which physical format for Bronze?** It must be readable column-wise by
   later phases (pandas/dbt loads, local and S3-compatible), partitionable by
   date for incremental loads, and free of engine lock-in.
2. **Who enforces quality, and when?** Contracts already *declare* types,
   nullability, volume, uniqueness, ranges and referential rules — something
   must actually *run* them, and its verdict must be machine-readable.
3. **What happens on failure?** A gate that silently drops data or that
   aborts the whole run are both unacceptable; failed data must remain
   inspectable without ever reaching consumers.
4. **How is a batch identified idempotently?** Re-running must not duplicate,
   overwrite or diverge — without any catalog (no Hive metastore, no Iceberg).

## Decision

1. **Bronze = snappy Parquet parts in `retail-bronze/bronze/…`, partitioned by
   `year=/month=/` only for the three large fact sources** (`sales`,
   `purchases`, `inventory` — their date column is the dominant scan filter).
   Dimensions and small sources are a single object per batch; tiny inventory
   producing many small parts is accepted in exchange for a uniform,
   incremental-friendly layout.
2. **Every part carries five provenance columns** appended after the contract
   columns: `source`, `source_file`, `source_hash` (sha256 of the raw bytes),
   `ingestion_run_id`, `ingestion_timestamp`. Any row can be traced to its
   exact raw object without external metadata.
3. **Quality is enforced by Great Expectations at Bronze**, suites built
   *programmatically per batch* from three layers: generic contract rules
   (volume, not-null, types, PK uniqueness), each contract's
   `quality.declared` rules mapped 1:1 onto concrete expectations, and
   cross-source referential checks against the dimension key sets. Ephemeral
   GX context — no persisted checkpoint state to go stale.
4. **Promotion is marker-based:** a batch is consumable only if
   `_promoted/<source>/<batch>.json` exists. `status = passed` requires **zero
   GX failures and zero standardization-rejected rows**. On failure: RAW
   untouched, Parquet parts retained for debugging, quality report written,
   rejected rows dumped to `_rejected/`, **no marker** — never promoted,
   never discarded. Downstream must read only promoted batches.
5. **Strict standardization, explicit typing.** Unparsable values exclude the
   row with a coded issue (`invalid_number`, `null_violation`, …) — never a
   silent coercion (`"n/a"` → `0` is forbidden). Nullable columns become
   pandas extension dtypes (`Int64`/`Float64`/`boolean`); dates become
   `datetime64[ns]`; a missing required column is fatal for the batch.
6. **Batch identity = raw content hash.** The raw filename stem
   (`sales__<hash16>`) is the batch id, so part keys, quality reports and
   markers are deterministic: reruns are `already_bronzed`, and quality is
   re-evaluated on every run (a property of the data, not of one write).

## Why Parquet, and why not the alternatives

| Alternative | Why not chosen |
|-------------|----------------|
| CSV/TSV Bronze | No types, no column projection, ~5–10× larger and slower; re-parsing reintroduces the ambiguity RAW deliberately keeps. |
| Delta Lake / Apache Hudi / Iceberg | Bring a transaction log, engine coupling and JVM/Spark weight this 7.8 GiB local host does not need; content-addressed part keys already give us idempotent, immutable batches. |
| JSON-lines Bronze | Preserves nothing typed, bloated, and pushes parsing cost to every reader. |
| Database-staged Bronze (PostgreSQL first) | Mixes landing with serving; failures would require table cleanup instead of simply writing no marker; object storage is the phase boundary the platform is built on. |
| A Metastore / catalog for partitions | `year=/month=/` in the key is legible to any S3 client and to dbt later; a catalog is an operational dependency with no payoff at this scale. |
| `dt=YYYY-MM-DD` day partitioning | The synthetic facts span years but Bronze batches are whole raw files; month granularity keeps part counts sane (spec allows choosing the partition column). |

## Why Great Expectations, and why built programmatically

| Alternative | Why not chosen |
|-------------|----------------|
| dbt tests at this stage | dbt needs the warehouse loaded; quality must gate *before* data leaves MinIO. dbt tests remain the right tool for Silver/Gold models. |
| Pandera / pandera-df | Fine for DataFrame schemas, but no natural place for cross-source referential checks or the required machine-readable report with `expectations_total/passed/failed`. |
| Hand-rolled checks | Would reinvent result structures, statistics and failure detail — and the roadmap mandates Great Expectations anyway; using it at Bronze proves the integration early. |
| Persisted GX checkpoint/context | Files under `gx/` would accumulate state, drift from the code and break hermetic CI; suites derived from contracts are code, so an ephemeral context is always consistent. |
| Contracts' `quality.declared` as *data* executed by a custom engine | Duplicates GX's machinery and splits the quality story in two; mapping the declared lines onto GX expectations keeps one runner, one report format. |

## Consequences

- **Positive:** a single, machine-readable gate satisfies every required
  quality rule (volume, types, nullability, ranges, uniqueness, referential
  integrity) with one report schema per batch; failed data is fully auditable
  and reprocessable while consumers provably never see it; provenance columns
  make every Parquet row traceable to immutable RAW bytes; content-addressed
  keys make the whole layer idempotent; benchmarks give honest baselines
  (~5.7k rows/s small, ~10.4k rows/s medium end-to-end including GX).
- **Negative:** promotion markers are a *convention* — any reader that bypasses
  `_promoted/` (or lists `bronze/` directly) can still see unvalidated parts;
  per-batch GX runs add tens of seconds at small scale (accepted: batches are
  daily-batch sized, not streaming); partitioned sources emit many small parts
  for small batches (inventory → 48 parts at tiny scale) — refiled as a future
  compaction concern; referential checks are skipped *with a note* when a
  dimension is absent, so early ingestion order still matters.
- **Known GX quirks encoded in the code:** date-only bounds must be passed as
  `datetime` (pydantic coerces `date` and pandas cannot compare it against
  `datetime64[ns]`); `run.results` order is not the suite order (failures are
  matched by expectation type + column); `…ToBeInSet` treats null as
  unexpected (harmless — all FK columns are non-nullable); type expectations
  are only emitted for numpy dtypes (extension dtypes are enforced by the
  strict standardizer instead).
- **Follow-ups:** load promoted batches into PostgreSQL Silver; compact small
  partitioned parts; extend the suite to `raw_external/` sources (weather, FX,
  products) once their contracts gain declared rules; move suites to a
  persistent GX store only if a UI/asset story appears; wire `bronze` into the
  Airflow DAG (`bronze_build` is already sketched in
  `docs/architecture/orchestration.md`).
