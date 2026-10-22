WITH orders_riyadh AS (
    SELECT 
        order_id,
        store_id,
        CAST(created_at::TIMESTAMP + INTERVAL '3 hours' AS DATE) AS order_date,
        EXTRACT(WEEK FROM (created_at::TIMESTAMP + INTERVAL '3 hours')) AS iso_week
    FROM {{ source('raw', 'orders') }}
    WHERE status = 'delivered'
),
line_items AS (
    SELECT
        oi.line_no AS line_id,
        o.iso_week,
        s.city,
        p.category,
        o.order_date,
        oi.quantity,
        ROUND(oi.quantity * oi.unit_price - oi.discount, 2) AS line_revenue_sar,
        ch.unit_cost_sar
    FROM {{ source('raw', 'order_items') }} oi
    JOIN orders_riyadh o ON oi.order_id = o.order_id
    JOIN {{ source('raw', 'stores') }} s ON o.store_id = s.store_id
    JOIN {{ source('raw', 'products') }} p ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('product_cost_history') }} ch
        ON TRIM(p.sku) = ch.sku
       AND ch.valid_from <= o.order_date
       AND (ch.valid_to IS NULL OR o.order_date < ch.valid_to)
    WHERE oi.quantity > 0
      AND o.order_date >= '2026-08-31'
)
SELECT
    iso_week,
    city,
    category,
    COUNT(line_id) AS lines,
    ROUND(SUM(line_revenue_sar), 2) AS revenue_sar,
    ROUND(SUM(CASE WHEN unit_cost_sar IS NOT NULL THEN quantity * unit_cost_sar ELSE 0 END), 2) AS cost_sar,
    ROUND(SUM(CASE WHEN unit_cost_sar IS NOT NULL THEN line_revenue_sar - (quantity * unit_cost_sar) ELSE 0 END), 2) AS margin_sar,
    COUNT(CASE WHEN unit_cost_sar IS NULL THEN 1 END) AS uncosted_lines
FROM line_items
GROUP BY iso_week, city, category
HAVING COUNT(line_id) > 0