-- granicus:
--   time_column: ordered_at
--   interval_unit: day
--   layer: staging

SELECT
    order_id,
    customer_id,
    CAST(order_total AS NUMERIC) AS order_total,
    TIMESTAMP(created_at) AS ordered_at,
    status
FROM `my-gcp-project.raw.orders`
WHERE status != 'cancelled'
