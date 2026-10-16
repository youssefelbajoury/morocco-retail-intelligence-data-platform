# ADR-003 — Keyless external APIs, verbatim raw caching

- **Status:** Accepted
- **Date:** 2026-10-07
- **Phase:** 05 — External API connectors (raw layer only)
- **Deciders:** Project author (single-developer portfolio project)

---

## Context

Phases 00–04 delivered the foundation, a deterministic synthetic retailer, the
local container stack and a contract-gated file ingestion path into MinIO
`raw/`. The roadmap then calls for *external* data — weather, exchange rates and
product-catalog enrichment — but Phase 05 had to stop at the raw layer: no
bronze normalization, no database, no warehouse, no quality suites yet.

Four questions had to be settled before writing a single connector:

1. **Which providers?** Three datasets are needed later (weather for demand
   patterns, EUR/MAD + USD/MAD + GBP/MAD for cost normalization, product
   attributes for the product dimension) and the platform must run without any
   secret management while it has no scheduler and no key vault.
2. **How does the raw layer stay honest and re-runnable?** API responses are
   mutable on the provider side; raw storage must be immutable, and rerunning
   the same command must not produce duplicates or surprise rewrites.
3. **How do we stay polite?** The providers are free but rate-limited
   (Open Food Facts: 15 product reads/min/IP), and a naive retry loop can turn
   one transient error into a ban.
4. **Where does reconciliation stop?** Internal synthetic products have
   `product_id`s like `P000001` but no barcode column, while Open Food Facts is
   keyed by barcode — enrichment must not mutate the internal catalog.

## Decision

1. **Exactly three keyless providers, no API keys anywhere:**
   - **Open-Meteo** (`archive-api` for history, `api` for forecast) — daily
     weather per region/city with `timezone=Africa/Casablanca`;
   - **Frankfurter v2** (`api.frankfurter.dev`) — `base=EUR`,
     `quotes=MAD,USD,GBP` (MAD verified against `/v2/currencies`);
   - **Open Food Facts v3** (`world.openfoodfacts.org`) — product reads by
     barcode with a custom User-Agent carrying a contact address.
   Every metadata file records `auth: {"mode": "none"}`, so the absence of
   credentials is auditable, and all base URLs are env-overridable.
2. **Raw responses are stored verbatim** as `response.json` next to a
   `metadata.json` provenance sidecar (endpoint, sorted params, sha256, HTTP
   facts, client behaviour, provider-specific context). Nothing is rewritten,
   parsed-at-write or normalized in this phase.
3. **Idempotence is request-addressed:**
   `raw_external/source=<source>/retrieved_date=<day>/request_hash=<sha256>/…`,
   where the hash covers *URL + sorted parameters*. A rerun on the same day is
   `already_fetched` (no HTTP at all); the next day produces a fresh fetch under
   a new partition — time-varying data is re-observed, identical requests are
   never duplicated within a day.
4. **Bounded, polite HTTP:** max 4 attempts, exponential backoff
   (`0.5·2ⁿ`, 30 s cap, ±25 % jitter), `Retry-After` honoured up to 60 s,
   retry only `429/5xx` (+ Cloudflare `520–524`) and transport failures,
   **never** a permanent 4xx or a malformed payload; per-client pacing of 1.0 s
   (weather, FX) and 4.1 s (Open Food Facts ≈ 14.6 req/min under its 15/min cap).
5. **A failed request never aborts the run:** it becomes a `failed` record with
   a machine-readable reason, the remaining requests continue, and the CLI exits
   `1`.
6. **Dry-run plans without side effects:** `--dry-run` computes requests, keys
   and outcomes (`planned`) but performs no HTTP call, no object write and no
   manifest — so the command surface can be inspected in CI for free.
7. **Product mapping is exact-barcode and read-only.** `MATCHED` / `NEW` /
   `UNMATCHED` mapping lives inside the product `metadata.json`; the internal
   product data is never modified, and no fuzzy matching is allowed.
8. **Tests never hit the network.** Unit and integration suites inject a
   transport/fake sleep; one separate opt-in smoke suite
   (`MRI_LIVE_SMOKE=1`) performs a single tiny live call per provider.

---

## Why keyless providers only

- **No secrets, no rot, no vault.** The platform has no scheduler and no secret
  store yet; a key in `.env` would be the first credential with production
  blast radius and no rotation story.
- **Reproducible CI.** Anyone can run the smoke test without provisioning
  accounts; the normal suite needs no network at all.
- **Honest scope.** The three datasets needed by later phases (weather, FX,
  product enrichment) are all available keyless with clear, published terms;
  anything requiring a key belongs to a phase that also introduces secret
  management.

## Why verbatim caching under a request hash instead of "fetch and upsert"

| Property | Upsert on every run | Request-addressed raw cache |
|----------|--------------------|-----------------------------|
| Raw immutability | Provider rewording silently replaces history | Every observation keeps its own immutable key |
| Rerun cost | Always hits the API (rate-limit risk) | Same-day rerun is a local existence check |
| Time-varying data | Overwrites the previous day | New `retrieved_date` partition per day |
| Auditability | "What exactly did the API answer that day?" is lost | `response.json` + `metadata.json` answer it byte-for-byte |
| Provider outage | Run fails | Today's data already stored → `already_fetched` |

The `retrieved_date` partition (not the provider's date) is deliberate: it makes
the *observation* time explicit, keeps the day a stable hash component, and
gives later phases a natural incremental-load boundary.

## Why retries are bounded and asymmetric

Retrying a permanent `400` is pointless and hostile (it repeats a request the
provider already rejected), so only transient classes are retried. Backoff with
jitter prevents thundering herds when several connectors run in one batch, and
honouring `Retry-After` respects the provider's explicit ask. Pacing (not
bursts with retries) is what keeps Open Food Facts' 15 req/min/IP budget safe —
the 4.1 s default deliberately sits just under the 4 s-per-request ceiling.
Everything (sleep, clock, transport) is injectable so tests assert this policy
without ever sleeping or going online.

## Why the phase stops at raw JSON

Bronze typing, quality suites and warehouse loading have their own contracts and
gates downstream. Storing the provider's exact bytes first means the later
normalization step can always be re-derived and diffed against what was actually
received — the same reason ADR-002 content-addresses files instead of parsing
them on write.

---

## Alternatives considered

| Alternative | Why not chosen |
|-------------|----------------|
| Keyed providers (e.g. WeatherAPI, OpenExchangeRates) | Introduce credentials, quotas and rotation with no benefit for the three datasets needed. |
| Scraping retailer sites for products | ToS risk, brittle markup, and Open Food Facts already exposes the same data as an open API. |
| Fetch-and-upsert into a single "latest" object | Violates raw immutability and loses the day-by-day history later freshness checks will need. |
| Content-hash-only keys (as in Phase 04) | The response body changes daily for the same request, so content hashing alone would conflate *when* something was observed. |
| Caching responses on local disk only | Bypasses the S3-compatible boundary the platform is designed against and hides data from other phases. |
| Retrying every error | Turns permanent client errors and rate bans into repeated traffic; hides contract bugs. |
| Fuzzy product matching (name/token similarity) | Ambiguous matches silently corrupt enrichment; exact barcode equality is the only defensible rule. |
| Writing product mappings into the internal product data | Violates "internal data is generated, read-only for connectors". |
| One combined client for all providers | Different URLs, payload shapes, pacing and validation rules; a shared `httpx` transport gives the reuse that matters. |
| Polling Open Food Facts search instead of direct reads | Search is capped at 10 req/min and returns weaker payloads than the product endpoint. |

---

## Consequences

- **Positive:** no secrets anywhere; idempotent, auditable raw caching with
  day-partitioned provenance; a polite, provider-respecting HTTP layer whose
  policy is unit-tested offline; enrichment that cannot corrupt internal data;
  and a CLI that degrades gracefully (per-request failures, exit codes 0/1/2).
- **Negative:** raw keys are long and hash-suffixed (mitigated by manifests and
  metadata); historical weather requests depend on Open-Meteo's archive lag
  (~5 days) so very recent "history" must come from the forecast endpoint; the
  same request re-fetched tomorrow creates a second object by design (storage
  grows with observations, not with code); Open Food Facts coverage for Moroccan
  products may leave many barcodes `UNMATCHED`.
- **Follow-ups:** feed `raw_external/` into the bronze layer (typed, partitioned
  Parquet), turn the declared limits into Great Expectations suites, add the
  `barcode,product_id` column to generated products so `MATCHED` becomes
  meaningful, and schedule the daily fetch once Airflow exists.
