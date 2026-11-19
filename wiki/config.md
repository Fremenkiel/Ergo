# Config

All fields must be set through one of the tree options.

Toml config is loaded first, then overridden by any present env variable.
Cli args will override both toml and env.

All entries are optional for a given type, but all config options must be set for the program to start. 

## Toml 

```toml
[postgres]
host = "localhost"
port = 5432
database = "db"
user = "db_rw"
password = "12345678"
sslmode.verify_full = "./ca.crt"
slot = "wal_slot"
publication = "db_pub"

[clickhouse]
host = "localhost"
port = 8443
database = "db"
user = "default"
password = "clickhouse"
secure = true

[replication]
batch_size = 500
flush_interval_ms = 360
```

### Ssl mode

```toml
# None
sslmode = "off"

# Require
sslmode = "require"

# Verify full without cert
sslmode.verify_full = ""

# Verify full with cert
sslmode.verify_full = "./ca.crt"
```

## Env

```env
ERGO_POSTGRES_HOST=localhost
ERGO_POSTGRES_PORT=5432
ERGO_POSTGRES_DATABASE=db
ERGO_POSTGRES_USER=postgres
ERGO_POSTGRES_PASSWORD=postgres
ERGO_POSTGRES_SSLMODE_TYPE=verify_full
ERGO_POSTGRES_SSLMODE_PATH=./ca.crt
ERGO_POSTGRES_SLOT=wal_slot
ERGO_POSTGRES_PUBLICATION=db_pub

ERGO_CLICKHOUSE_HOST=localhost
ERGO_CLICKHOUSE_PORT=8443
ERGO_CLICKHOUSE_DATABASE=db
ERGO_CLICKHOUSE_USER=default
ERGO_CLICKHOUSE_PASSWORD=clickhouse
ERGO_CLICKHOUSE_SECURE=true

ERGO_REPLICATION_BATCH_SIZE=500
ERGO_REPLICATION_FLUSH_INTERVAL_MS=360
```

### Ssl mode

```env
# None
ERGO_POSTGRES_SSLMODE_TYPE=off

# Require
ERGO_POSTGRES_SSLMODE_TYPE=require

# Verify full without cert
ERGO_POSTGRES_SSLMODE_TYPE=verify_full

# Verify full with cert
ERGO_POSTGRES_SSLMODE_TYPE=verify_full
ERGO_POSTGRES_SSLMODE_PATH=./ca.crt
```

## Cli args

```bash
--config ./config.toml
--postgres-host localhost
--postgres-port 5432
--postgres-database db
--postgres-user db_rw
--postgres-sslmode verify_full=./ca.crt
--postgres-slot wal_slot
--postgres-publication db_pub

--clickhouse-host localhost
--clickhouse-port 8443
--clickhouse-database db
--clickhouse-user default
--clickhouse-secure clickhouse

--replication-batch-size 500
--replication-flush-interval-ms 360

--help
--test-pause-at=transaction_received_before_enqueue
```

### Ssl mode

```bash
# None
--postgres-sslmode off

# Require
--postgres-sslmode require

# Verify full without cert
--postgres-sslmode verify_full

# Verify full with cert
--postgres-sslmode verify_full=./ca.crt
```
