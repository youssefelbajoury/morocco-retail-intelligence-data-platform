# Release readiness — Morocco Retail Intelligence Data Platform

> **Phase 27 — final GitHub release preparation.**
> Status: **PARTIAL — ready to publish once the owner makes the decisions in §10.**
> Preparation only: **no commit, push, remote, tag or publish was performed.**

This is the single-page go/no-go checklist for the first public release. It
aggregates what is done, what is proven locally, what is explicitly *not* proven,
and the exact owner actions required before publishing.

---

## 1. Completed features

All implemented and covered by tests/docs (see `ARCHITECTURE.md` status table):

- Deterministic synthetic generator (internal source).
- Versioned source contracts — **14** contracts (`data_contracts/*.yml`, v1.0.0).
- File ingestion + **3 keyless public API** connectors (Open-Meteo, Frankfurter,
  Open Food Facts) with provenance, retries, rate limits.
- RAW → BRONZE Parquet behind a **Great Expectations** quality gate.
- SILVER load into PostgreSQL (`raw_staging` / `silver` / `audit`) with
  `load_audit` registry + reconciliation.
- Kimball **star schema** — 7 dimensions + 3 facts (`mri.warehouse`).
- **dbt** analytics layer — 26 models, SCD **Type-2** `dim_produit`, incremental
  facts, **140 tests**, Python↔dbt parity test.
- **Apache Airflow 3.3.2** DAG `morocco_retail_pipeline` — 8 correlated tasks.
- **Apache Superset 6.1.0** BI — read-only `superset_bi` role, 4 datasets,
  **42 charts**, **5 dashboards** with native filters; seed + verify tooling.
- Structured audit logging + pipeline metrics (`ops` schema) and **data lineage**
  (JSON / OpenLineage / Mermaid).
- **Prometheus + Grafana** optional compose profile (4 dashboards, alert rules).
- Least-privilege PostgreSQL roles + secret rotation (Phases 23–24).
- **GitHub Actions CI** — 8 jobs (configured, not yet run remotely).
- Portfolio meta-docs: README, ARCHITECTURE, CONTRIBUTING, CHANGELOG,
  troubleshooting runbook, limitations/roadmap, data dictionary, 14 ADRs.

## 2. Local validation evidence

| Gate | Result | Source |
|------|--------|--------|
| Secret scan (worktree + HEAD) | 29 heuristic hits, **all benign** (see §5) | Phase 27 (live) |
| `ruff check .` | **pass** | Phase 27 (live) |
| `ruff format --check .` | **pass** — 209 files | Phase 27 (live) |
| `uv run mypy` | **pass** — 140 source files | Phase 27 (live) |
| `uv run pytest tests/unit -q` | **459 passed** (66.4 s) | Phase 27 (live) |
| Full `uv run pytest` (unit+integration+smoke) | **504 passed, 3 skipped** (4294 s) | Phase 24 (cited) |
| Playwright/Edge E2E (Superset filters) | **8 passed** (350 s) | Phase 25 (cited) |
| Compose config — 4 combinations | **valid** (exit 0) | Phase 27 (live) + Phase 26 |
| Clean start on isolated stack | **READY** | Phase 26 (cited) |
| Markdown link/path check | **186/186 local links resolve** (59 files) | Phase 27 (live) |

> **Evidence provenance.** Phase 27 changed **no Python and no compose/env
> configuration**, so the full integration/dbt/e2e suites were **not re-run**; the
> figures above are the last validated runs (Phase 24 / 25 / 26). The live checks
> in Phase 27 confirm there is no drift (lint/types/unit green, compose valid).

## 3. GitHub CI status

| Item | Status |
|------|--------|
| Workflow file | `.github/workflows/ci.yml` — **present, untracked** |
| Jobs | **8** (lint, typecheck, unit, integration-postgres, dbt, minio-integration, airflow-dag, docker) |
| Secrets required | **None** (disposable dummy credentials inline) |
| Remote runs | **0 — never executed on GitHub** |
| Badge | Present but uses placeholder owner **`OWNER`**; not green |
| **Branch mismatch** | Workflow triggers on **`main`**; the local default branch is **`master`** → CI would not trigger after a naive push |

**The CI badge must not be described as passing until a real remote run succeeds.**

## 4. Clean-start status

- **Phase 26: READY.** The full platform was reproduced from a clean, isolated,
  non-destructive scratch stack (`mri-cs`): generator → ingestion → Bronze →
  Silver → warehouse → dbt (`PASS=167 ERROR=0`) → Airflow (8/8 tasks success) →
  Superset (42 charts / 5 dashboards, verify all green), with scratch resources
  removed afterwards and the live stack left healthy.
- **Not performed:** a *destructive* rebuild of an existing data volume
  (`down -v` on the real stack). Treated as an accepted, documented limitation.

## 5. Security status & limitations

- `.env` is **git-ignored and untracked**; only `.env.example` (empty placeholders)
  is tracked. Generated data, E2E artifacts, `dbt/target`, `dbt/logs`, `reports/`
  and `airflow/logs/*` are ignored.
- No tracked secret, private key, connection string, DB or archive exists
  (verified by extension + content heuristics).
- Secret scanner: `scripts-ignored/scan_secrets.py` → 29 heuristic hits; a
  line-level redacting scan (Phase 27) shows **all 55 line hits are benign** —
  variable names, `env_var()`/config references, SQL/CI command examples, and the
  ignored live `.env` file itself.
- **Open security items (documented, not blockers):**
  - The Airflow **Fernet key / JWT secret** shown in an earlier transcript were
    recommended for rotation (Phase 24 follow-up); not yet rotated.
  - ~371 leftover disposable **test schemas** remain in the live `retail` database
    (no drops performed).
  - The host is RAM-constrained (~3.7 GiB Docker VM); the two full stacks cannot
    run simultaneously.

## 6. Remaining unverified UI behaviour

- Superset **native-filter** interactive behaviour is now **verified** (Phase 25,
  Edge/Playwright) — previously the one open acceptance item (AC11).
- Still unverified / not captured: interactive behaviour beyond the covered
  filters, **other browsers** (only Chromium-based Edge was exercised), the
  Airflow UI beyond DAG execution, and Grafana UI rendering. No additive UI
  screenshots (Airflow run, Grafana, lineage) were fabricated;
  the 5 Superset screenshots are real.

## 7. Actual benchmark evidence

Raw evidence committed under `benchmarks/{bronze,silver,dbt,queries,pipeline}/results.json`
(measured on the dev host, synthetic data; illustrative, not SLAs):

- Full cold pipeline (medium, ingest → dbt): **≈ 12.6 min**.
- Bronze: **~10 400 rows/s** (medium); Silver: **~2 300 rows/s** (medium).
- Parquet vs raw CSV (medium): 92.7 MB → 23.6 MB (**≈ 3.9×**).
- dbt incremental vs full: 1 m sales 76.5 s → **29.5 s** (2 % delta).

## 8. Repository hygiene snapshot (Phase 27)

- Branch: `master`. Commits: **1** (`e82c36d` — foundation/containers only).
- **No git remotes configured.**
- Working tree: **285 file-level changes** — 21 modified, 6 deleted (obsolete
  `.gitkeep`), 1 staged rename (`A Propos.txt` → `docs/archive/project-brief.fr.txt`),
  ~257 untracked (the accumulated Phase 01–26 implementation).
- Docs consistency gaps found (report-only): README roadmap / ARCHITECTURE status
  stop at **Phase 22** (missing Phases 23–26); CHANGELOG ends at Phase 22;
  README test count "497" is stale vs the latest **504**; README §"Configuring the
  CI badge" incorrectly says `OWNER` also appears in
  `docs/architecture/orchestration.md`; `.env.example` line 2 has a double-encoded
  em dash (mojibake).

## 9. Exact requirements for publication

Publish only when **all** of the following are true:

1. **License chosen** (§10, owner decision) and `LICENSE` created; README + badge +
   `pyproject.toml` + `CONTRIBUTING.md` updated accordingly.
2. **Remote URL supplied** by the owner; replace both `OWNER` placeholders in the
   README badge (the repo slug `morocco-retail-intelligence` is currently a guess).
3. **Branch reconciled:** either rename the default branch to `main` (matching the
   workflow trigger + badge) **or** change `.github/workflows/ci.yml` and the badge
   to `master`.
4. **Commit plan executed** (see Phase 27 report §8) — the repository currently has
   only one commit; the entire platform is uncommitted.
5. **First remote CI run is green** on the default branch; only then enable/claim
   the badge.
6. Owner confirms whether to keep the repository **public or private**.
7. (Recommended) Apply the doc-consistency fixes in §8 and rotate the Airflow
   Fernet/JWT secrets.

## 10. Outstanding owner decisions

| # | Decision | Options |
|---|----------|---------|
| D1 | **License** | MIT · Apache-2.0 · no license / private (`docs/reports/license-decision.md`) |
| D2 | **Remote URL / slug** | Real `owner/repo` (README badge + repo name) |
| D3 | **Visibility** | public · private |
| D4 | **Default branch** | `main` (rename) · `master` (edit workflow+badge) |
| D5 | **Commit strategy** | grouped history (9 commits) · single squash commit |
| D6 | `scripts-ignored/scan_secrets.py` | track it (move to `scripts/`) · keep untracked · gitignore |
| D7 | Doc-consistency fixes (§8) | apply now · defer |

## 11. Status

**PARTIAL (go, once §9/§10 are resolved).** Nothing here blocks a portfolio
review; the blockers are the owner-controlled publication decisions (license,
remote URL, branch, commit authorization).
