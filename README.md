# Ergo
Postgres audit log 

## refs
https://github.com/facebook/zstd
https://github.com/lz4/lz4/archive/refs/heads/dev.zip

## Be avare
Each LowCardinality insert can at max contain 65,536 unique column names.

## Emit
SELECT/PERFORM pg_logical_emit_message(
    true, 
    'ergo_meta', 
    '{"user_id": 42, "ip": "192.168.1.50"}'
);
### OBS
No json parse func, string has to be this.

## Credits
https://github.com/karlseguin/pg.zig
https://github.com/0xrinegade/clickhouse-zig

## Setup
Flag sql setup, roles, table setup.
### pg
wal_level=logical

## OBS
Do not supprt PG 14+ with explicit streaming = 'on' flag.
This results in the rows being streamed as they come in and not when the transaction is commited. 
Having this flag on could result in data loss and rolled back rows being logged.

## Needed for test
psql
clickhouse-client


# Gen SSL
openssl genrsa -out ca.key 2048
openssl req -new -x509 -days 3650 -key ca.key -out ca.crt -subj "/CN=LocalPostgresCA"
openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/CN=localhost"
openssl x509 -req -in server.csr -days 3650 \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt
chmod 600 server.key

# Build
## With SSL (MacOs and homebrew)
zig build -Dopenssl=true \
  -Dopenssl_lib_path=/opt/homebrew/opt/openssl@3/lib \
  -Dopenssl_include_path=/opt/homebrew/opt/openssl@3/include
