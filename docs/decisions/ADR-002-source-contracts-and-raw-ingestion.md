# ADR-002 — Source contracts and content-addressed raw ingestion

- **Status:** Accepted
- **Date:** 2026-10-06
- **Phase:** 04 — File ingestion layer
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phases 00–03 produced the project foundation, a deterministic synthetic retail
generator (10 entities, CSV/JSON output) and a running local container stack
(PostgreSQL 16 + MinIO). The first data path — *files on disk → MinIO raw
storage* — had to be built without yet using Airflow, dbt, a warehouse schema or
a full Great Expectations suite.

Three questions had to be settled:

1. **How does a file prove it belongs to the platform?** The Phase 01 policy in
   `data_contracts/README.md` required a versioned contract per source, but no
   contract format had been chosen and no contract existed yet.
2. **How do we re-run ingestion safely?** Raw data must never be silently
   overwritten, yet the pipeline must be idempotent and auditable.
3. **Where does validation stop in Phase 04?** Column/type checks are cheap and
   belong at the entry point; referential and range rules belong to the quality
   gate that comes with Great Expectations.

## Decision

1. **Contracts are YAML, one file per source, in `data_contracts/<source>.yml`.**
   Every contract carries `id`, semantic `version` (currently `1.0.0`), `owner`,
   `source`, `format`, `keys.primary`, a declared `columns` list
   (`name`/`type`/`required`/`nullable`/`description`), `freshness`, `volume`,
   `quality.declared` and `sla`.
2. **Contract versions are semantic and major-version-checked.** The loader
   parses `MAJOR.MINOR.PATCH` and refuses any contract whose major version is
   not understood (`1.x` today).
3. **Raw objects are content-addressed:**
   `raw/<source>/<stem>__<sha256[:16]><ext>`.
4. **Invalid files are quarantined, not deleted:** they land in
   `rejected/contracts/<source>/…` plus a `.reason.json` sidecar, and the run
   manifest records them as `rejected`.
5. **One manifest per run**, written to `reports/ingestion/<run_id>.json` and
   uploaded to `raw/_manifests/<run_id>.json`, containing counts and one record
   per file (source, hash, size, contract id/version, row count, outcome, keys,
   reason).
6. **Validation scope in Phase 04 = format, required columns, null-safety and
   basic types** over a bounded sample, with the full row count streamed.
   Everything else is declared in `quality.declared` but not executed here.

---

## Why YAML for contracts

- **Readable and diff-friendly in Git**, which matters because contracts are
  versioned and reviewed like code.
- **PyYAML is already in the dependency tree**, so no new tooling is introduced
  for parsing.
- **A YAML file can hold the whole policy** (keys, freshness, volume, quality
  declarations) in one place, whereas a Python or JSON schema would either lose
  the non-schema sections or force a second document.
- YAML keeps schema concerns separate from the JSON Schema formats the Great
  Expectations suites will consume later.

## Why content-addressed keys instead of overwriting `raw/<source>/<file>.csv`

| Property | Overwrite-by-name | Content-addressed |
|----------|-------------------|-------------------|
| Re-running the same file | Trivially idempotent, but writes anyway | Detected as `already_ingested`, no write |
| A *different* file with the same name | Silently destroys the previous raw object | Lands under a different key — nothing lost |
| Auditability | One object, history only via versioning | Every payload has its own immutable key |
| Deduplication | None | Identical content is stored once |

This is the concrete mechanism behind the architectural rules *"raw is
immutable"* and *"no silent data loss"*. A downside is that keys are less human
friendly, mitigated by keeping the original `<stem>` in the name and by the
`original_filename` field in the manifest.

## Why rejected files are kept

Rejecting without keeping the file makes the failure unreproducible and forces
the producer to re-send data the platform has already seen. Storing the original
bytes plus a machine-readable `.reason.json` (contract id/version, hash, every
issue with column and row) lets a producer fix and re-submit, while the
`rejected/` prefix guarantees a broken payload can never be mistaken for valid
raw data.

## Why validation stops where it does

- **Cheap and structural checks belong at entry** — they fail fast and keep bad
  payloads out of the raw layer.
- **Uniqueness, ranges and referential integrity need full scans or joins**
  and are better expressed as executable expectations. Encoding them twice (once
  in this validator, once in Great Expectations) guarantees drift, so they are
  *declared* in the contract and *executed* only by the quality gate.
- Type checks are bounded to the first 1000 data rows to keep validation linear
  in memory; the row count is still a full stream count for volume reporting.

---

## Alternatives considered

| Alternative | Why not chosen |
|-------------|----------------|
| JSON Schema as the contract format | Poor for human-reviewed policy sections (freshness, SLA), no comments, noisier diffs. |
| Contracts as Python classes | Executable code in place of a data agreement; harder for a producer to read or for CI to lint. |
| One contract file containing all sources | Forces a single version for every entity; a breaking change in one source would bump all of them. |
| Overwriting `raw/<source>/<file>.csv` | Violates "raw is immutable"; a changed file silently replaces history. |
| Rejected files deleted after logging | Unreproducible failures and a broken producer loop; violates "no silent data loss". |
| Enforcing full data quality at ingestion | Duplicates the Great Expectations layer, needs full scans, and makes ingestion slow and brittle. |
| `boto3` / `s3fs` instead of `minio` | Heavier dependency tree for a single S3 client; `minio` is the official lightweight SDK. |
| Skipping MinIO entirely (filesystem first) | Would not exercise the S3-compatible boundary the rest of the platform is designed against. |

---

## Consequences

- **Positive:** a machine-checkable entry gate, immutable raw objects with
  trivially idempotent re-runs, fully auditable outcomes per run, and contract
  versioning that consumers can pin with `--expect-version`.
- **Negative:** raw keys contain hashes (less readable), contract files must be
  kept in sync with the generator schema by hand, and type checks cover only a
  sample of rows.
- **Follow-ups:** encode `quality.declared` as Great Expectations suites
  (done — Phases 06/07), compile contracts into JSON Schema (open), and
  reconcile contract ids with the planned external sources (`weather_daily`,
  `exchange_rates`, `product_catalog_ext`) when connectors arrive
  (**resolved by [`ADR-005`](ADR-005-external-bronze-contracts.md)**: the
  four external sources received their own contracts — `open_meteo_history`,
  `open_meteo_forecast`, `frankfurter_fx`, `openfoodfacts_products` — rather
  than sharing entity-shaped ids).