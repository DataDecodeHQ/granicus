-- granicus:layer=staging
-- granicus:grain=order_id

SELECT
    order_id,
    customer_id,
    ordered_at,
    total_amount,
    status
FROM `my-gcp-project.raw.orders`
WHERE ordered_at >= CURRENT_DATE() - 30
