-- granicus:
--   depends_on: [int_order_summary]
--   layer: entity
--   grain: customer_id

SELECT
    s.customer_id,
    s.total_orders,
    s.lifetime_spend,
    s.first_order_at,
    s.last_order_at,
    DATE_DIFF(CURRENT_DATE(), DATE(s.last_order_at), DAY) AS days_since_last_order,
    CASE
        WHEN s.total_orders >= 10 THEN 'high'
        WHEN s.total_orders >= 3 THEN 'medium'
        ELSE 'low'
    END AS order_frequency_tier
FROM int_order_summary s
