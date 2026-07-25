# What

It important to monitor the stored WAL, to make sure nothing are about to go wrong.

```sql

SELECT
  slot_name,
  plugin,
  database,
  restart_lsn,
  CASE
    WHEN invalidation_reason IS NOT NULL THEN 'invalid'
    ELSE
      CASE
        WHEN active IS TRUE THEN 'active'
        ELSE 'inactive'
      END
    END as "status",
  pg_size_pretty(
    pg_wal_lsn_diff(
      pg_current_wal_lsn(), restart_lsn)) AS "retained_wal",
  pg_size_pretty(safe_wal_size) AS "safe_wal_size"
FROM
  pg_replication_slots
ORDER BY slot_name;

```
