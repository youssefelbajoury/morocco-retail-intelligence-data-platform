# Security & hardening (Phase 20)

This page records the security posture of the Morocco Retail Intelligence Data
Platform: the threats it assumes, the controls it relies on, the evidence that
those controls work, and the gaps that remain. It is written for a
**single-developer, host-local development deployment** — not a production
certification.

## 1. Scope and threat assumptions

- **Asset**: a local platform (PostgreSQL warehouse, MinIO object store
  RAW/BRONZE layers, Apache Airflow orchestration, Apache Superset BI, an
  optional Prometheus/Grafana monitoring profile).
- **Trust boundary**: one Windows 11 host + Docker Desktop. The container
  network (`mri-net`) is a private bridge; container-to-container traffic is
  trusted; the host is trusted.
- **In scope**: credential handling, database least-privilege, container
  exposure, supply chain (images/dependencies), and application-level
  defaults (auth, debug, CSRF).
- **Out of scope**: multi-host scaling, external attackers with host access,
  OS-level hardening, TLS termination. The platform is not exposed to the
  public internet and must not be.

## 2. Secrets

| Control | Status |
|---------|--------|
| No secrets committed to git | Pass — one commit (`e82c36d`), no credential material; a heuristic scan of the working tree + `HEAD` found only placeholders |
| `.env` git-ignored and untracked | Pass — `.gitignore` ignores `.env`; `git ls-files .env` is empty |
| `.env.example` placeholders only | Pass — every value empty; documents the required keys |
| Secrets delivered via env vars | Pass — no password/key literals in compose, Dockerfiles, DAGs or application code |
| No secrets in reports/docs | Pass — regression test scans tracked deliverables for credential-shaped strings |
| Dependency vulnerability scan | Pass — `uv audit`: no known vulnerabilities across 99 packages |

The real `git-ignored` `.env` holds the platform's own local credentials
(`POSTGRES_*`, `MINIO_*`, `AIRFLOW_*`, `SUPERSET_*`, `BI_READER_*`); they are
never printed or copied into tracked files.

**Phase 23 rotation** (2026-10-12): the `retail_app` warehouse password, the
MinIO root secret and the Grafana admin secret were all rotated to fresh 64-char
values (details + verification in §12). No other `.env` key aliases the rotated
values (`DBT_TARGET_PASSWORD` is absent from `.env`; the dbt wrapper injects
`POSTGRES_PASSWORD`; `BI_READER_*` and `AIRFLOW_POSTGRES_*` are separate roles),
and the Airflow containers never bake the secrets — they read the mounted `.env`
at task runtime, so no service recreation was required for Airflow.

**Phase 24 addition**: the new break-glass administrator credential is stored only
in the git-ignored `.env` as `POSTGRES_ADMIN_USER`/`POSTGRES_ADMIN_PASSWORD`
(both empty placeholders in `.env.example`). The application role's password is
unchanged, so no application/BI wiring changed.

## 3. Database privileges

Two PostgreSQL instances exist and are deliberately separate:

- `mri-postgres` (`retail`) — the warehouse.
- `mri-airflow-postgres` (`airflow`) — Airflow **metadata only**, never used
  for warehouse data.

Roles in `retail` (PHASE 24 — least privilege):

| Role | Purpose | Verified properties |
|------|---------|---------------------|
| `retail_app` | application + database **owner** (pipeline/dbt DDL) | LOGIN; owns the `retail` database and all user objects; **NOT superuser**, no CREATEDB/CREATEROLE/REPLICATION/BYPASSRLS; `pg_monitor` not needed |
| `retail_admin` | break-glass administrator | LOGIN, **SUPERUSER**; owns nothing (no user objects); used only for role/database provisioning + recovery; never wired into the app/BI |
| `superset_bi` | Superset BI reads | LOGIN, not superuser, no CREATEROLE/CREATEDB, **no role memberships**, USAGE (not CREATE) on `warehouse`/`dbt_marts`/`audit`, **SELECT-only** table grants, `allow_dml=False` at the Superset layer |

Why ownership (not SUPERUSER) is sufficient: every DDL statement the platform
issues (warehouse builder, silver loader, observability `ops` schema, dbt
materialisations) targets objects/schemas `retail_app` already owns; no
`CREATE EXTENSION` is used and the only CREATEDB/CREATEROLE needs are one-time
provisioning, now the admin role's job. The canonical role model is
[`infrastructure/sql/least-privilege-roles.sql`](../../infrastructure/sql/least-privilege-roles.sql).

Read-only role grants are provisioned by
[`infrastructure/sql/bi-reader-readonly.sql`](../../infrastructure/sql/bi-reader-readonly.sql)
(idempotent; `ALTER DEFAULT PRIVILEGES FOR ROLE retail_app` covers tables created
later). Enforcement is tested live: `superset_bi` can `SELECT` the analytics
schemas and its `CREATE TABLE` is refused
(`tests/integration/test_bi_role_readonly.py`); the non-superuser owner split is
asserted by `tests/integration/test_least_privilege_roles.py`.

## 4. Apache Airflow

- **Secrets**: `AIRFLOW__CORE__FERNET_KEY` (44-char, valid) and
  `AIRFLOW__API_AUTH__JWT_SECRET` set via env; the metadata database was checked
  to hold **0 connections and 0 variables** (no ambient credentials at rest).
- **Auth**: FAB auth manager, single admin account; API/UI reachable on
  `127.0.0.1:8080` only.
- **DAG**: no secret literals; validated with the real Airflow `DagBag`
  (`tests/airflow/validate_dag.py`).
- **DNS**: the compose overlay pins public resolvers `8.8.8.8`/`1.1.1.1` — a
  documented, measured mitigation for a Docker Desktop embedded-resolver outage
  (ADR-009 §8, `docs/orchestration/airflow.md`, report p14). Retained as-is.
- **Mounts**: `airflow/dags`, `airflow/logs` and the repository are mounted into
  the containers (write needed for logs/pipeline); acceptable on a local host.

## 5. Docker / container exposure

- **Loopback-only ports**: every published host port is bound to `127.0.0.1`
  (`postgres:5433`, `minio:9000/9001`, `airflow:8080`, `airflow-postgres:5434`,
  `superset:8088`, `metrics:9245`, `prometheus:9090`, `grafana:3001`). Nothing is
  reachable from the LAN.
- **Pinned images**: no `latest` anywhere; project images (`mri-airflow`,
  `mri-superset`, `mri-metrics`) are built from pinned bases and versioned.
- **Healthchecks**: all ten running services define a healthcheck.
- **Network**: a single private bridge (`mri-net`); no extra published networks.
- **Resource limits**: intentionally not set — the stack runs inside the Docker
  VM budget and has been stable across phases; adding caps risks OOM-killing the
  pipeline on this host (documented decision, §9).

## 6. MinIO supply chain (CVE-2025-62506)

MinIO stopped publishing community Docker images (community edition is now
source-only); Docker Hub and Quay tags are gone. The previously pinned
`minio/minio:RELEASE.2025-09-07T16-13-09Z` is affected by **CVE-2025-62506**
(privilege escalation via session-policy bypass, CVSS 8.1) and is no longer
pullable from any registry.

The default is now the maintained **Chainguard** rebuild, digest-pinned:

```
cgr.dev/chainguard/minio@sha256:dc455cdd4db6e390b1b26514f15ff37681a777bf8dab41aac445a0259f751e56
# RELEASE.2026-09-22T19-25-18Z (post CVE-2025-62506)
```

Validated in isolation (temp ports/creds) against the four MinIO
integration-test modules (6 passed) before adoption, then re-verified against
the recreated service; the `mri-minio-data` volume is mounted unchanged and was
snapshotted first. See ADR-001 for migration/rollback details.

> **Residual exposure note**: the platform uses only root MinIO credentials and
> defines no restricted service/STS accounts, so the CVE's exploitation path
> (a *restricted* account escalating) was not reachable in this deployment —
> but the affected build is no longer used regardless.

## 7. Apache Superset

- Runs under **gunicorn** (production WSGI); no Flask debug/reloader
  (`FLASK_ENV`/`FLASK_DEBUG` unset).
- `SUPERSET_SECRET_KEY` is a generated value from `.env` (session/CSRF signing);
  the metadata store (SQLite) is a named volume.
- Admin account `admin` created from `.env`.
- Warehouse connection logs in as `superset_bi` with `allow_dml=False`; verified
  read-only end-to-end. The seed/verify tooling drives the API with a **Bearer
  token + CSRF header** flow.

## 8. Dependencies and images

- Python: single lockfile (`uv.lock`), `uv audit` clean (99 packages).
- Containers: exact tags (`postgres:16-alpine`, `prom/apache` pinned versions)
  and a digest-pinned MinIO; DAG/CI never reference removed images.

## 9. Known gaps and accepted risks

1. ~~**`retail_app` is a superuser**~~ — **RESOLVED in Phase 24**. The
   application role is now a non-superuser *owner*: ownership (not SUPERUSER) is
   what the pipeline's DDL needs. The mandatory SUPERUSER attribute was kept only
   on the new, otherwise-unused break-glass role `retail_admin` (see §14).
2. ~~**Grafana default `admin/admin`**~~ — **RESOLVED in Phase 23**. The compose
   overlay no longer wires any `admin/admin` default
   (`GF_SECURITY_ADMIN_USER`/`GF_SECURITY_ADMIN_PASSWORD` are mandatory
   `${GRAFANA_ADMIN_USER}`/`${GRAFANA_ADMIN_PASSWORD}` interpolations with no
   `:-default`); the stored admin password was rotated and verified via Basic
   auth (`GET /api/user` returns 200 only for the new secret).
3. **No resource limits** on containers (see §5).
4. **Container `dns:` pins** public resolvers unconditionally (see §4); a future
   change could make them env-overridable, but the current behavior is the
   measured fix for a real outage.
5. **Benchmark/integration schemas** (`*_itinc*`, `*_bench*`, `*_e2e_*`) linger
   in the retail database from prior test runs. Phase 23 inventoried them (no
   drops, as scoped): **162 leftover schemas / 1,386 tables / ≈12.7M estimated
   rows**, dominated by `*_e2e_*` (3 each of `dbt_marts`/`dbt_snapshots`/
   `warehouse`/`silver`, ~2.99M rows each) and the incremental-append
   `*_itinc*`/`*_inc*` families (~1.4M est rows total). Harmless (Superset only
   queries the real `warehouse`/`dbt_marts`/`audit` schemas) but noisy; clean-up
   is deferred as a Phase 24 maintenance item. Also observed: the active set is
   small (`raw_staging`, `silver`, `warehouse`, `dbt_marts`, `dbt_snapshots`,
   db `dbt_*` staging/intermediate shells, `audit`, `ops`, empty `public`).

## 10. What Phase 20 changed

- MinIO image → digest-pinned Chainguard build (compose, CI, docs, ADR-001);
  CI MinIO job converted from an unpullable `services:` container to a
  `docker run` step.
- All published ports → loopback-only across the four compose overlays.
- Restored the missing `AIRFLOW_*`/`SUPERSET_*`/`BI_READER_*` keys in `.env`
  (required by Compose) and re-synced the Airflow metadata password and
  `superset_bi` password to `.env`; Superset metadata reset + re-seeded (the new
  `SECRET_KEY` could not decrypt the prior store).
- Added regression tests: `tests/unit/test_security_hardening.py` (offline
  config/secrets contract) and `tests/integration/test_bi_role_readonly.py`
  (live read-only enforcement).

## 11. How to verify

```powershell
uv run pytest tests/unit/test_security_hardening.py tests/integration/test_bi_role_readonly.py tests/integration/test_least_privilege_roles.py -q
docker compose --env-file .env -f infrastructure/docker/docker-compose.yml config -q
git check-ignore .env        # prints ".env"
docker ps --format "{{.Ports}}"   # every published port starts with 127.0.0.1
uv audit                     # no known vulnerabilities
```

## 12. What Phase 23 changed (credential rotation)

Rotated the three exposed local credentials and re-verified the whole stack
against the new values. All rotations are reversible and left no data changes.

| Credential | Method | Post-rotation verification |
|------------|--------|----------------------------|
| PostgreSQL `retail_app` | `ALTER ROLE retail_app WITH PASSWORD` (64-char; login + superuser preserved) | new secret connects (`apps.dbt debug` OK; host + Airflow-container psycopg `SELECT current_user` = `retail_app`); previous secret rejected |
| MinIO root secret | new `MINIO_ROOT_PASSWORD` applied via service restart (source-verified: `cmd/common-main.go` reloads root creds from env on every boot) | new secret lists all 12 buckets; previous secret rejected; before/after object counts identical (+8 raw / +400 bronze only from the Phase 23 E2E ingest) |
| Grafana admin | stored password reset via `grafana cli admin reset-admin-password` (secret passed over container stdin) | `GET /api/user` Basic auth: new → 200 (`admin`, `isGrafanaAdmin=true`); `admin/admin` → 401. (The `/api/login` form endpoint returns 401 even for valid creds through HTTP scripts/CSRF — Basic auth is the valid verification path.) |

Other facts:

- The compose `observability` overlay now interpolates
  `GF_SECURITY_ADMIN_USER: ${GRAFANA_ADMIN_USER}` /
  `GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD}` (mandatory, no
  `:-admin` default); `.env.example` documents both keys as required.
- Compose gotcha (recovered from): `docker compose` resolves `.env` from the
  **project directory** (`infrastructure/docker`), **not** the working
  directory — every Compose invocation must pass
  `--env-file <repo>/.env`. The first restart attempt omitted it and MinIO
  crash-looped on missing `MINIO_ROOT_USER`; the service was recreated with the
  flag and data is intact.
- Host CLI regression under the new secrets: fresh `tiny` dataset
  `ingest` → `bronze` completed (raw 267→275, bronze 1355→1755), Superset
  `verify.py` → all checks passed.
- Airflow-side regression: from `mri-airflow-scheduler` with the DAG venv
  Python, the mounted `.env` + container-env overrides connect to PostgreSQL
  (`current_user` = `retail_app`) and MinIO (12 buckets, `retail-raw` = 275
  objects) with the rotated secrets.

## 13. What Phase 24 changed (least-privilege PostgreSQL roles)

`retail_app` was a superuser only because it was the *initdb bootstrap* login.
PostgreSQL refuses to strip SUPERUSER from that role and refuses to rename the
role in use by the current session, so the hand-off was done as:

1. rename bootstrap `retail_app` → `retail_admin` from a short-lived helper
   superuser session (keeps the mandatory SUPERUSER attribute; becomes the
   break-glass admin);
2. create a brand-new non-superuser `retail_app` (same password, so no `.env`
   application change was needed);
3. `ALTER DATABASE retail OWNER TO retail_app` and transfer every *user*
   relation/function/type/schema to it (system catalogs and table-linked
   sequences untouched — the owning table's `ALTER` cascades to its sequence);
4. re-apply `superset_bi` SELECT-only grants + `ALTER DEFAULT PRIVILEGES FOR
   ROLE retail_app`; rotate the admin password into the git-ignored `.env` as
   `POSTGRES_ADMIN_USER`/`POSTGRES_ADMIN_PASSWORD`.

Post-migration state (live): `retail_app` attrs
`(rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls)` =
all **false**; it still owns the `retail` database, 2,018 tables, 810 views, 82
sequences, 1,072 indexes and 370 user schemas; `retail_admin` is the only
superuser and owns 0 user objects. `pg_monitor` was **not** granted: a measured
probe showed `pg_stat_activity` row/state visibility is identical for the
superuser and an unprivileged role, so the metrics exporter needs no extra
grant.

Verified end-to-end as the demoted role: warehouse build (TRUNCATE + reload +
reconcile, 10 tables), `dbt build` (167/167 PASS, creating schemas/views/tables/
incremental models/snapshot), observability `ops` metrics, and the metrics
exporter scrape (`last_scrape_success 1`, retail errors 0). `superset_bi`
remains SELECT-only, including default privileges on freshly created tables.
`CREATE DATABASE` and `CREATE ROLE` are refused. A disposable-database test ran
the same battery before touching the live cluster.

## 14. Phase 24 remaining items (not executed)

1. Rotate the remaining local credentials (`BI_READER_*`, Superset admin,
   Airflow admin/JWT/Fernet) on the Phase 23 pattern. The Airflow Fernet/JWT
   values were transiently visible in an earlier tool transcript and are the
   highest-priority rotation.
2. Prune the 162 inventoried stale schemas from `retail` (backup first).
3. Decide whether to rename the initdb bootstrap for **fresh** clusters so the
   compose init runs as `retail_admin` directly, removing the one-time hand-off
   (tracked as a follow-up; current clusters are already converged).
