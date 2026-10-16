-- Morocco Retail Intelligence — read-only role for Apache Superset (PHASE 12).
--
-- Superset connects as BI_READER_USER (NEVER retail_app) and may only read the
-- analytics schemas it is meant to chart:
--
--   warehouse   star schema (7 dims + 3 facts)
--   dbt_marts   dbt re-expression of the same star schema (parity-tested)
--   audit       load_audit registry (pipeline/health dashboards)
--
-- Apply from the repository root (PowerShell):
--
--   Get-Content infrastructure/sql/bi-reader-readonly.sql | docker exec -i mri-postgres `
--     psql -U retail_app -d retail -v ON_ERROR_STOP=1 `
--     -v reader_user="$env:BI_READER_USER" -v reader_password="$env:BI_READER_PASSWORD" -f -
--
-- POSIX:
--   BI_READER_USER=... BI_READER_PASSWORD=... \
--   psql -U retail_app -d retail -v ON_ERROR_STOP=1 \
--     -v reader_user="$BI_READER_USER" -v reader_password="$BI_READER_PASSWORD" \
--     -f infrastructure/sql/bi-reader-readonly.sql
--
-- Idempotent: the role is created only if missing; GRANTs may be re-applied
-- at any time. Default privileges cover tables the pipeline rebuilds later.

-- Matches POSTGRES_DB in .env; edit here only if the database is renamed.
\set reader_db 'retail'
-- Owner of the analytics tables whose future tables the reader must see.
-- Matches POSTGRES_USER in .env; explicit `FOR ROLE` makes the default
-- privileges correct even when applied by the admin role.
\set app_user 'retail_app'

-- CREATE ROLE with a password literal cannot live inside a DO block, because
-- psql does not substitute variables in dollar-quoted strings. \gexec runs the
-- CREATE only when the role is missing.
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'reader_user', :'reader_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'reader_user')
\gexec

GRANT CONNECT ON DATABASE :"reader_db" TO :"reader_user";
GRANT USAGE ON SCHEMA warehouse, audit, dbt_marts TO :"reader_user";
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse, audit, dbt_marts TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"app_user" IN SCHEMA warehouse GRANT SELECT ON TABLES TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"app_user" IN SCHEMA audit GRANT SELECT ON TABLES TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"app_user" IN SCHEMA dbt_marts GRANT SELECT ON TABLES TO :"reader_user";
