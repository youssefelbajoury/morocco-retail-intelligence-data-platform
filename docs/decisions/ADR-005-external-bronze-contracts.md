# ADR-005 — External Bronze: dedicated contracts and sidecar-gated normalization

- **Status:** Accepted
- **Date:** 2026-10-07
- **Phase:** 07 — External Bronze (raw_external → Parquet + quality gate)
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phase 05 landed three keyless providers as **verbatim** JSON under
`raw_external/source=<src>/retrieved_date=<day>/request_hash=<sha256>/`, each
response with a `metadata.json` provenance sidecar — by design raw stays
source-faithful and unvalidated. Phase 06 built the Bronze gate for the ten
`raw/` sources and explicitly deferred the external family ("extend the suite
to `raw_external/` sources once their contracts gain declared rules",
ADR-004 follow-ups).

Four questions had to be settled before that follow-up could close:

1. **Do external payloads get contracts at all?** Nothing may enter the
   platform without one — yet the stored objects are nested provider
   payloads (`daily` arrays, rate lists, one product + mapping), not flat
   rows.
2. **If so, whose contracts?** The repo policy says *one contract per
   logical entity, regardless of provenance* (`data_contracts/README.md`
   §4) — but the FX rates are not the `sales` entity, and the Open Food
   Facts fetch is not the internal `products` dimension.
3. **Who reshapes verbatim JSON into rows?** Fetching (Phase 05) must stay
   dumb; Bronze (Phase 06) assumed flat CSV/JSON. The transformation needs
   an owner that cannot corrupt RAW.
4. **What gates promotion for these sources?** Weather measures are
   nullable, products may legitimately be *not found*, and mapping status
   lives in the sidecar, not the payload.

## Decision

1. **Four new dedicated contracts**, ids equal to the storage source ids:
   `open_meteo_history`, `open_meteo_forecast`, `frankfurter_fx`,
   `openfoodfacts_products` (all `v1.0.0`, `format.types: [json]`,
   `owner: external-api`). Contract id == source id is what makes the
   dispatcher's lookup total — no alias table, no guesswork. This
   deliberately deviates from "one contract per logical entity": external
   contracts describe the **fetch result** (request-shaped, provenance
   stamped), internal ones describe the **entity**. Enrichment later joins
   them (`external_product_id` → internal `product_id`) instead of merging
   the contracts.
2. **Normalization is a pure function between RAW and the existing
   machinery.** `external_bronze.normalize_external(source, payload_bytes,
   metadata_bytes, contract) -> bytes` reshapes one response+sidecar pair
   into flat, all-text rows whose keys are exactly the contract columns and
   feeds the *unchanged* Phase-06 pipeline (`parse_rows` → strict
   standardizer → Parquet+provenance → GX → marker). Structural problems
   raise `SourceFileError` (batch fails, quality report, no parts); cell
   problems stay with the standardizer and the gate. RAW is never written.
3. **The metadata sidecar is mandatory.** A response without
   `metadata.json` is `SourceFileError` ("missing metadata sidecar") — no
   parts, no promotion. Provenance is a gate condition, not documentation.
4. **Batch id = `<source>__sha256(full response key)[:16]`.** The response
   key already embeds a request hash, but the uniform rule is "batch id =
   raw object stem", so external batches reuse `batch_id_for` with an
   `external_prefix` argument and behave identically at Bronze (content
   derived → `already_bronzed` reruns, deterministic part/marker keys).
   The `metadata.json` sibling is never discovered as a batch.
5. **Declared rules are executed like any other source.** The four
   contracts' `quality.declared` lines map 1:1 onto GX expectations in
   `quality.py`; weather additionally joins the existing referential layer
   (`region_id → regions`), and the nullable `tmax ≥ tmin` pair runs with
   `ignore_row_if="either_value_is_missing"` so an incomplete day skips
   instead of failing. One runner, one report schema, one promotion rule —
   external batches are otherwise indistinguishable from internal ones
   downstream.

## Why dedicated contracts, and why not the alternatives

| Alternative | Why not chosen |
|-------------|----------------|
| Reuse internal contracts (`products`, a new `weather_daily`, …) | Verbatim payloads do not have entity keys or entity grain (one FX response is many rates; one product response is one barcode); forcing them through internal contracts would require shape-changing at fetch time and would conflate provenance domains. |
| Normalize at fetch time (Phase 05 writes flat JSON) | Breaks the raw-is-verbatim invariant Phase 05 was built on: the stored object would no longer be what the provider returned, and provenance/quality bugs would be unrecoverable without a refetch. |
| Store external responses under `raw/<source>/` | `_list_raw` parses `<stem>__<hash16>` + format from the internal layout; the sidecar-per-response layout and `retrieved_date`/`request_hash` partitioning are a distinct contract domain with its own idempotence story. |
| Trust the payload for mapping status (`NEW`/`UNMATCHED`) | The sidecar is the record of what the client resolved; payload-only derivation loses `MATCHED` and would let a payload edit silently rewrite enrichment decisions. Hence sidecar-first, derivation only as fallback. |
| Drop the sidecar requirement (normalize from payload alone) | Weather region/timezone context and product mapping status do not exist in the payload; accepting sidecar-less responses would produce rows with invented or missing context. |
| A separate "external bronze" pipeline/command | Would duplicate discovery, standardization, GX, promotion and idempotence — two gates to keep in sync forever. Extending `BronzePipeline._discover`/`_process` around one normalizer keeps one gate. |

## Consequences

- **Positive:** external batches get the full Phase-06 guarantees — typed
  Parquet, five provenance columns (with `source_file` pointing at the exact
  `response.json`, whose sidecar carries the HTTP facts), machine-readable
  quality reports, marker-only consumption, byte-identical RAW, idempotent
  reruns (live-validated: run 2 = `already_bronzed=5`, full sweep =
  `already_bronzed=16`); downstream Silver loading needs no code that knows
  where a batch came from.
- **Negative:** the "one contract per logical entity" policy now has a
  documented exception (four fetch-shaped contracts alongside ten
  entity-shaped ones), so readers must check ADR-005 before assuming id ==
  entity; every external response requires its sidecar to exist — an
  operator who manually uploads a lone `response.json` gets a failed batch;
  `quality.py` grows a per-source declared-rule table that must be kept in
  sync with the YAML `quality.declared` strings (mismatches surface as a
  "declared but not executed" gap, not as a test failure).
- **Known GX quirks encoded:** `ExpectColumnValuesToBeInSet` skips nulls
  (nullability is enforced by the not-null expectation instead);
  `ExpectColumnPairValuesAToBeGreaterThanB` only skips *double*-missing rows
  by default, so nullable pairs must pass `either_value_is_missing`
  explicitly (pydantic rejects the intuitive `either_value_is_null` name);
  type expectations are not emitted for pandas extension dtypes (`Int64`,
  `Float64`) — nullable external measures are typed by the standardizer.
- **Follow-ups:** Silver loads must consume `_promoted/` markers for the
  four external sources like any other; compact/renumber when product
  fetches grow; revisit the declared-rule sync (a test that parses the YAML
  and diffs against `quality.py`'s strings) if the contract set changes
  often; wire external batches into the Airflow `bronze_build` task; dbt
  models join `openfoodfacts_products` to `products` via
  `external_product_id` mapping.
