-- granicus:
--   depends_on: [stg_orders]
--   time_column: ordered_at
--   interval_unit: day
--   layer: intermediate

SELECT
    customer_id,
    COUNT(*) AS total_orders,
    SUM(order_total) AS lifetime_spend,
    MIN(ordered_at) AS first_order_at,
    MAX(ordered_at) AS last_order_at
FROM stg_orders
GROUP BY customer_id
