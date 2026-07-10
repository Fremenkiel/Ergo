CREATE TABLE IF NOT EXISTS erp_audit_log
(
    event_time DateTime64(3, 'UTC'),
    transaction_id UInt64,
    user_id String,
    table_name LowCardinality(String),
    action Enum8('INSERT' = 1, 'UPDATE' = 2, 'DELETE' = 3),

    changed_columns Array(String),
    old_values Map(String, String),
    new_values Map(String, String),

    ip_address IPv4
)
ENGINE = MergeTree()
-- The ORDER BY clause is your Primary Key. Order matters immensely.
-- This sorts data physically on disk for fastest retrieval.
ORDER BY (table_name, event_time, user_id);

-- If a specific column ever needs to be read a lot for e.g. analytics, use cl materualized columns 
-- ALTER TABLE erp_audit_log 
-- ADD COLUMN invoice_total Decimal(10,2) 
-- MATERIALIZED CAST(new_values['total'] AS Decimal(10,2));
