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

## OBS
Do not supprt PG 14+ with explicit streaming = 'on' flag.
This results in the rows being streamed as they come in and not when the transaction is commited. 
Having this flag on could result in data loss and rolled back rows being logged.

## Needed for test
psql
