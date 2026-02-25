-- Fails if the table is empty after materialization.
SELECT COUNT(*) AS row_count
FROM stg_api_events
HAVING COUNT(*) = 0
