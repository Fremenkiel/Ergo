DO $$
BEGIN
    -- Migrator
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_migrator') THEN
        CREATE ROLE db_migrator LOGIN PASSWORD '12345678';
    END IF;

    -- Read / Write
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_rw') THEN
        CREATE ROLE db_rw LOGIN PASSWORD '12345678';
    END IF;

    -- Read only
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_ro') THEN
        CREATE ROLE db_ro LOGIN PASSWORD '12345678';
    END IF;

    -- Read only / publication
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_rp') THEN
        CREATE ROLE db_rp LOGIN PASSWORD '12345678';
    END IF;

    -- No password
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_np') THEN
        CREATE ROLE db_np LOGIN;
    END IF;

    -- Read only / scram sha256
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_ro_scram_sha256') THEN
        CREATE ROLE db_ro_scram_sha256 LOGIN PASSWORD '12345678';
    END IF;

    -- Read only / ssl
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'db_ro_ssl') THEN
        CREATE ROLE db_ro_ssl LOGIN PASSWORD '12345678';
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

CREATE TYPE custom_enum AS ENUM ('val1', 'val2');

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

CREATE TABLE simple_table (value text);
ALTER TABLE simple_table REPLICA IDENTITY FULL;

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

-- Readonly no password user
GRANT USAGE ON SCHEMA public TO db_np;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_np;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO db_np;

-- Readonly scram sha256 user
GRANT USAGE ON SCHEMA public TO db_ro_scram_sha256;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_ro_scram_sha256;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO db_ro_scram_sha256;

-- Readonly ssl
GRANT USAGE ON SCHEMA public TO db_ro_ssl;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO db_ro_ssl;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO db_ro_ssl;

-- Default privileges for FUTURE objects created by db_migrator
ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO db_rw;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT ON TABLES TO db_ro;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT ON TABLES TO db_rp;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT ON TABLES TO db_np;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT ON TABLES TO db_ro_scram_sha256;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT ON TABLES TO db_ro_ssl;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO db_rw;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO db_ro;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO db_rp;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO db_np;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO db_ro_scram_sha256;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT ON SEQUENCES TO db_ro_ssl;

-- Allow connections
GRANT CONNECT ON DATABASE db TO db_migrator, db_rw, db_ro, db_rp, db_np, db_ro_scram_sha256, db_ro_ssl;

-- db publication
CREATE PUBLICATION db_pub FOR ALL TABLES;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'wal_slot') THEN
        PERFORM pg_create_logical_replication_slot('wal_slot', 'pgoutput');
    END IF;
END
$$;

        -- \\ drop table if exists all_types;
        -- \\ create table all_types (
        -- \\   id integer primary key,
        -- \\   col_int2 smallint,
        -- \\   col_int4 integer,
        -- \\   col_int8 bigint,
        -- \\   col_float4 float4,
        -- \\   col_float8 float8,
        -- \\   col_bool bool,
        -- \\   col_text text,
        -- \\   col_bytea bytea,
        -- \\   col_int2_arr smallint[],
        -- \\   col_int4_arr integer[],
        -- \\   col_int8_arr bigint[],
        -- \\   col_float4_arr float4[],
        -- \\   col_float8_arr float[],
        -- \\   col_bool_arr bool[],
        -- \\   col_text_arr text[],
        -- \\   col_bytea_arr bytea[],
        -- \\   col_enum custom_enum,
        -- \\   col_enum_arr custom_enum[],
        -- \\   col_uuid uuid,
        -- \\   col_uuid_arr uuid[],
        -- \\   col_numeric numeric,
        -- \\   col_numeric_arr numeric[],
        -- \\   col_timestamp timestamp,
        -- \\   col_timestamp_arr timestamp[],
        -- \\   col_json json,
        -- \\   col_json_arr json[],
        -- \\   col_jsonb jsonb,
        -- \\   col_jsonb_arr jsonb[],
        -- \\   col_char char,
        -- \\   col_char_arr char[],
        -- \\   col_charn char(3),
        -- \\   col_charn_arr char(2)[],
        -- \\   col_timestamptz timestamptz,
        -- \\   col_timestamptz_arr timestamptz[],
        -- \\   col_cidr cidr,
        -- \\   col_cidr_arr cidr[],
        -- \\   col_inet inet,
        -- \\   col_inet_arr inet[],
        -- \\   col_macaddr macaddr,
        -- \\   col_macaddr_arr macaddr[],
        -- \\   col_macaddr8 macaddr8,
        -- \\   col_macaddr8_arr macaddr8[]
