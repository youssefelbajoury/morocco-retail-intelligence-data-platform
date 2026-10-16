-- Morocco Retail Intelligence — least-privilege PostgreSQL roles (PHASE 24).
--
-- Target model
-- ------------
--   POSTGRES_ADMIN_USER  (retail_admin)  break-glass SUPERUSER. Owns nothing
--                                        except the built-in catalogs. Used only
--                                        for provisioning roles/databases and
--                                        manual recovery. NEVER used by the app.
--   POSTGRES_USER        (retail_app)    application + database OWNER. Owns the
--                                        `retail` database, every user object
--                                        and issues all pipeline DDL
--                                        (CREATE SCHEMA/TABLE/INDEX/CONSTRAINT,
--                                        TRUNCATE, COPY, dbt materialisations).
--                                        NOT a superuser and holds no
--                                        createdb/createrole/replication/
--                                        bypassrls attribute. Ownership (not
--                                        SUPERUSER) is what makes the DDL work.
--   BI_READER_USER       (superset_bi)   SELECT-only reader for Superset
--                                        (see bi-reader-readonly.sql).
--
-- Why ownership is enough
-- -----------------------
-- Every DDL statement the platform issues (warehouse builder, silver loader,
-- observability `ops` schema, dbt) runs against objects/schemas the app role
-- already owns, so no elevated role attribute is required. `CREATE EXTENSION`
-- is never used. The only cluster-level privileges that used to need CREATEDB/
-- CREATEROLE are one-time provisioning steps, which now belong to the admin
-- role.
--
-- Applying this file
-- ------------------
-- Run as the ADMIN role (it creates/alters roles and transfers ownership):
--
--   PowerShell:
--     $env:PGPASSWORD = $env:POSTGRES_ADMIN_PASSWORD
--     Get-Content infrastructure/sql/least-privilege-roles.sql | docker exec -i mri-postgres `
--       psql -U "$env:POSTGRES_ADMIN_USER" -d retail -v ON_ERROR_STOP=1 `
--       -v app_user="$env:POSTGRES_USER" -v app_password="$env:POSTGRES_PASSWORD" `
--       -v reader_user="$env:BI_READER_USER" -f -
--
--   POSIX:
--     PGPASSWORD="$POSTGRES_ADMIN_PASSWORD" psql -U "$POSTGRES_ADMIN_USER" -d retail \
--       -v ON_ERROR_STOP=1 -v app_user="$POSTGRES_USER" \
--       -v app_password="$POSTGRES_PASSWORD" -v reader_user="$BI_READER_USER" \
--       -f infrastructure/sql/least-privilege-roles.sql
--
-- Idempotent: safe to re-run; it converges an existing cluster to the model.
--
-- NOTE on the live cluster's one-time hand-off
-- --------------------------------------------
-- PostgreSQL forbids removing SUPERUSER from the *initdb bootstrap* role and a
-- session cannot rename its own login role, so the live migration was performed
-- out-of-band (rename bootstrap retail_app -> retail_admin via a short-lived
-- helper superuser, then create the new non-superuser retail_app). That one-time
-- procedure, with its rollback, is documented in
-- docs/security/security-hardening.md. This file encodes the resulting
-- invariants and is what a fresh cluster should converge to.

\set target_db 'retail'

-- 1. Application/owner role (non-superuser). \gexec creates it only if missing.
SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE '
    'NOREPLICATION NOBYPASSRLS',
    :'app_user', :'app_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'app_user')
\gexec

-- 2. Make the app role the owner of the retail database.
ALTER DATABASE :"target_db" OWNER TO :"app_user";

-- 3. Move ownership of all *user* objects/schemas OWNED BY THE CURRENT ROLE
--    (the admin) to the app role. System catalogs (pg_*, information_schema)
--    and sequences linked to a table (deptype a/i) are left untouched — the
--    owning table's ALTER cascades to its linked sequence. When re-run by the
--    app role itself this is a no-op. `retail_app` is hardcoded because psql
--    variables are not interpolated inside dollar-quoted bodies; keep it in
--    sync with `app_user`.
DO $do$
DECLARE
    owner oid := (SELECT oid FROM pg_roles WHERE rolname = current_user);
    r record;
BEGIN
    FOR r IN SELECT n.nspname ns, c.relname rn, c.relkind rk
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE c.relowner = owner AND c.relkind IN ('r','v','m','S','f','p')
               AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
               AND NOT (c.relkind = 'S' AND EXISTS (
                   SELECT 1 FROM pg_depend d WHERE d.classid = 'pg_class'::regclass
                     AND d.objid = c.oid AND d.deptype IN ('a','i')))
    LOOP
        EXECUTE format('ALTER %s %I.%I OWNER TO %I',
            CASE r.rk WHEN 'S' THEN 'SEQUENCE' WHEN 'v' THEN 'VIEW'
                      WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'f' THEN 'FOREIGN TABLE'
                      ELSE 'TABLE' END, r.ns, r.rn, 'retail_app');
    END LOOP;

    FOR r IN SELECT p.oid FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
             WHERE p.proowner = owner
               AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
    LOOP
        EXECUTE format('ALTER FUNCTION %s OWNER TO %I', r.oid::regprocedure, 'retail_app');
    END LOOP;

    FOR r IN SELECT n.nspname ns, t.typname tn FROM pg_type t
             JOIN pg_namespace n ON n.oid = t.typnamespace
             WHERE t.typowner = owner AND t.typtype IN ('e','d','r')
               AND n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
    LOOP
        EXECUTE format('ALTER TYPE %I.%I OWNER TO %I', r.ns, r.tn, 'retail_app');
    END LOOP;

    FOR r IN SELECT nspname FROM pg_namespace
             WHERE nspowner = owner
               AND nspname !~ '^pg_' AND nspname <> 'information_schema'
    LOOP
        EXECUTE format('ALTER SCHEMA %I OWNER TO %I', r.nspname, 'retail_app');
    END LOOP;
END $do$;

-- 4. Read-only BI role defaults + grants FOR the app role (idempotent; the
--    CREATE ROLE for the reader itself lives in bi-reader-readonly.sql).
GRANT CONNECT ON DATABASE :"target_db" TO :"reader_user";
GRANT USAGE ON SCHEMA warehouse, audit, dbt_marts TO :"reader_user";
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse, audit, dbt_marts TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"app_user" IN SCHEMA warehouse GRANT SELECT ON TABLES TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"app_user" IN SCHEMA audit GRANT SELECT ON TABLES TO :"reader_user";
ALTER DEFAULT PRIVILEGES FOR ROLE :"app_user" IN SCHEMA dbt_marts GRANT SELECT ON TABLES TO :"reader_user";
