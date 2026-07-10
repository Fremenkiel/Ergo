-- db/init.sql
--
-- Cluster bootstrap. Run this against the `postgres` maintenance database BEFORE
-- goose migrations, because a CREATE DATABASE can't target the database it runs in.
--
--   psql "postgres://postgres:postgres@localhost:5432/postgres" -v ON_ERROR_STOP=1 -f db/init.sql
--
-- Creates the `admin` login role and the `db` database it owns. Idempotent:
-- safe to re-run against an already-bootstrapped cluster.

-- Admin login role (local dev credentials).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'admin') THEN
        CREATE ROLE admin LOGIN PASSWORD 'admin' CREATEDB;
    END IF;
END
$$;

-- Application database, owned by admin. CREATE DATABASE can't run inside a DO
-- block / transaction, so guard it with \gexec instead.
SELECT 'CREATE DATABASE db OWNER admin'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'db')\gexec

GRANT ALL PRIVILEGES ON DATABASE db TO admin;

