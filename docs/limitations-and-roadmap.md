# Limitations & Roadmap

Honest account of what the platform does and does not do. See also the
**Project status** section of [`README.md`](../README.md) and the Phase 21 audit
[`reports/final-acceptance.md`](reports/final-acceptance.md).

## Scope and environment

- **Host-local development deployment only.** One Windows 11 machine + Docker
  Desktop (WSL2); a single `mri-net` bridge. Nothing is bound beyond loopback.
  There is **no TLS, no authentication boundary and no multi-host scaling** —
  the stack must not be exposed to the internet.
- **RAM-constrained** (~7.8 GiB host, ~3.7 GiB Docker VM). Services are started
  in groups; running everything at once is tight.
- **Batch only.** The workload is daily batch; no streaming/CDC is implemented or
  planned.

## Data

- **Synthetic data only.** The generator is seeded and deterministic; external
  sources are public and keyless. Nothing here is real business data.
- The `holidays`-based calendar enrichment is **planned**, not implemented
  (`dim_date` carries a Ramadan flag and calendar attributes built from the
  synthetic calendar contract).

## Validated vs not

- **Validated locally:** full test suite, 4 Compose combinations, two green
  end-to-end DAG runs (idempotent), Superset verify (4 datasets / 42 charts /
  5 dashboards), Prometheus targets, lineage build + trace.
- **Not yet validated by GitHub Actions:** the CI workflow is committed but no
  remote run has succeeded; the badge owner is still a placeholder (`OWNER`).
- **Unverified:** interactive in-browser Superset native-filter behaviour (no
  browser-automation tooling). Filter wiring and the filter data path *are*
  verified.
- **Not re-run in the final audit:** a fully destructive clean-start rebuild
  (emptying an *existing* data volume). A **non-destructive** clean start was
  however proved on an isolated scratch stack in Phase 26
  ([`reports/p26.md`](reports/p26.md)); the rest is asserted from Compose
  validation, documentation and the healthy running stack.

## Known operational notes

- A transient `httpx.ReadTimeout` can affect the first ingest attempt of a DAG
  run under memory pressure; Airflow's automatic retry recovers it.
- MinIO community images were withdrawn upstream; a digest-pinned Chainguard
  rebuild is used (CVE-2025-62506 resolved).
- ~371 leftover test schemas in the `retail` database are harmless noise.

## Future roadmap

| Item | Notes |
|------|-------|
| `holidays` calendar enrichment | Moroccan holidays/Ramadan as first-class calendar attributes |
| MinIO object versioning | Immutable versioning on the raw bucket |
| Browser-automation filter smoke | Close the one unverified Superset item (e.g. Playwright) |
| Clean-start CI job | Scripted empty-volume rebuild on a scratch host |
| Deployment hardening | TLS, secrets management, multi-host (out of current scope) |
| `LICENSE` | Owner to approve a license (recommendation in [`reports/license-decision.md`](reports/license-decision.md), Phase 27) |
