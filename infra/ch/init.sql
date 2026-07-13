CREATE TABLE IF NOT EXISTS entries
(
    event_time DateTime64(3, 'UTC'),
    transaction_id UInt64,
    action Enum8('INSERT' = 1, 'UPDATE' = 2, 'DELETE' = 3),

    table_name LowCardinality(String),
    primary_key String,

    changed_columns Array(String),
    old_values Map(String, String),
    new_values Map(String, String),

    user_id String,
    ip_address IPv4
)
ENGINE = MergeTree()
-- TODO: This might need to be restructured based on read usage.
ORDER BY (table_name, primary_key, event_time, user_id);

-- If a specific column ever needs to be read a lot for e.g. analytics, use cl materualized columns 
-- ALTER TABLE erp_audit_log 
-- ADD COLUMN invoice_total Decimal(10,2) 
-- MATERIALIZED CAST(new_values['total'] AS Decimal(10,2));
