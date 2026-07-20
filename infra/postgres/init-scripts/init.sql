DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_migrator') THEN
        CREATE ROLE db_migrator LOGIN PASSWORD '12345678';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_rw') THEN
        CREATE ROLE db_rw LOGIN PASSWORD '12345678';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_ro') THEN
        CREATE ROLE db_ro LOGIN PASSWORD '12345678';
    END IF;

    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_rp') THEN
        CREATE ROLE db_rp LOGIN PASSWORD '12345678';
    END IF;
END
$$;

CREATE DATABASE db;

\connect db

-- Remove unsafe default privileges
REVOKE ALL ON DATABASE db FROM PUBLIC;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- Prisma requires the db_migrator to own the schema
ALTER SCHEMA public OWNER TO db_migrator;

-- db_Migrator full control
GRANT USAGE, CREATE ON SCHEMA public TO db_migrator;

-- Example tables
CREATE TABLE IF NOT EXISTS addresses (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  address_line_1 VARCHAR(255) NOT NULL,
  address_line_2 VARCHAR(255),
  postal_code VARCHAR(16) NOT NULL,
  city VARCHAR(255) NOT NULL,
  country VARCHAR(2) NOT NULL
  );
ALTER TABLE addresses REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS users (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email_address VARCHAR(255) NOT NULL UNIQUE,
  age INT NOT NULL,
  gender INT NOT NULL,
  address_id BIGINT REFERENCES addresses(id) ON DELETE CASCADE NOT NULL
  );
ALTER TABLE users REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS test_sync_marker (
  id BIGINT
  );
ALTER TABLE test_sync_marker REPLICA IDENTITY FULL;


-- Read/Write application user
GRANT USAGE ON SCHEMA public TO db_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO db_rw;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO db_rw;

-- Readonly user
GRANT USAGE ON SCHEMA public TO db_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_ro;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO db_ro;

-- Replication user
GRANT USAGE ON SCHEMA public TO db_rp;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_rp;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO db_rp;
ALTER ROLE db_rp REPLICATION;

-- Default privileges for FUTURE objects created by db_migrator
ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO db_rw;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT ON TABLES TO db_ro;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO db_rw;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO db_ro;

-- Allow connections
GRANT CONNECT ON DATABASE db TO db_migrator, db_rw, db_ro, db_rp;

-- db publication
CREATE PUBLICATION db_pub FOR ALL TABLES;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'wal_slot') THEN
        PERFORM pg_create_logical_replication_slot('wal_slot', 'pgoutput');
    END IF;
END
$$;
