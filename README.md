# Ergo
Postgres audit log 

## refs
https://github.com/facebook/zstd
https://github.com/lz4/lz4/archive/refs/heads/dev.zip

## Be avare
Each LowCardinality insert can at max contain 65,536 unique column names.

## Emit
SELECT pg_logical_emit_message(
    true, 
    'ergo_meta', 
    '{"user_id": 42, "ip": "192.168.1.50"}'
);
### OBS
No json parse func, string has to be this.
