-- Remove unsafe default privileges
DO $$
DECLARE 
	db_name VARCHAR(256);
BEGIN
    db_name := current_database();

    EXECUTE format(
        'REVOKE ALL ON DATABASE %I FROM PUBLIC;', 
        db_name
    );
    EXECUTE format(
        'REVOKE CREATE ON SCHEMA public FROM PUBLIC;', 
        db_name
    );
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO db_rw;', 
        db_name
    );
END
$$;

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

CREATE TABLE IF NOT EXISTS all_types (
  id INTEGER PRIMARY KEY,
  col_int2 SMALLINT,
  col_int2_arr SMALLINT[],
  col_int4 INTEGER,
  col_int4_arr INTEGER[],
  col_int8 BIGINT,
  col_int8_arr BIGINT[],
  col_float4 FLOAT4,
  col_float4_arr FLOAT4[],
  col_float8 FLOAT8,
  col_float8_arr FLOAT8[],
  col_bool BOOL,
  col_bool_arr BOOL[],
  col_text TEXT,
  col_text_arr TEXT[],
  col_bytea BYTEA,
  col_bytea_arr BYTEA[],
  col_enum custom_enum,
  col_enum_arr custom_enum[],
  col_uuid UUID,
  col_uuid_arr UUID[],
  col_numeric NUMERIC,
  col_numeric_arr NUMERIC[],
  col_timestamp TIMESTAMP,
  col_timestamp_arr TIMESTAMP[],
  col_json JSON,
  col_json_arr JSON[],
  col_jsonb JSONB,
  col_jsonb_arr JSONB[],
  col_char CHAR,
  col_char_arr CHAR[],
  col_charn CHAR(3),
  col_charn_arr CHAR(2)[],
  col_timestamptz TIMESTAMPTZ,
  col_timestamptz_arr TIMESTAMPTZ[],
  col_cidr CIDR,
  col_cidr_arr CIDR[],
  col_inet INET,
  col_inet_arr INET[],
  col_macaddr MACADDR,
  col_macaddr_arr MACADDR[],
  col_macaddr8 MACADDR8,
  col_macaddr8_arr MACADDR8[]
  );
ALTER TABLE all_types REPLICA IDENTITY FULL;

CREATE TABLE simple_table (value text);

CREATE TABLE IF NOT EXISTS test_sync_marker (
  id BIGINT
  );
ALTER TABLE test_sync_marker REPLICA IDENTITY FULL;

-- Read/Write application user
GRANT USAGE ON SCHEMA public TO db_rw;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO db_rw;
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO db_rw;
ALTER ROLE db_rw REPLICATION;

-- Default privileges for FUTURE objects created by db_migrator
ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO db_rw;

ALTER DEFAULT PRIVILEGES FOR ROLE db_migrator IN SCHEMA public
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO db_rw;

-- db publication
CREATE PUBLICATION db_pub FOR ALL TABLES;

DO $$
DECLARE 
  wal_name VARCHAR(256);
BEGIN
    wal_name := concat('wal_slot_', current_database());

    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = wal_name) THEN
      PERFORM pg_create_logical_replication_slot(wal_name, 'pgoutput');
    END IF;
END
$$;
