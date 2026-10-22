WITH valid_rows AS (
    SELECT 
        TRIM(s.sku) AS sku,
        CAST(s.valid_from AS DATE) AS valid_from,
        ROUND(
            CAST(
                REPLACE(
                    TRANSLATE(COALESCE(s.unit_cost, s.cost_sar), '٠١٢٣٤٥٦٧٨٩', '0123456789'), 
                    ',', '.'
                ) AS DOUBLE
            ), 2
        ) AS unit_cost_sar,
        s.source_file
    FROM {{ source('raw', 'supplier_prices_raw') }} s
    LEFT JOIN {{ ref('price_quarantine') }} q 
        ON s.source_file = q.source_file 
       AND TRIM(s.sku) = q.sku 
       AND CAST(s.valid_from AS DATE) = q.valid_from
    WHERE q.reason IS NULL
),
dedup_costs AS (
    SELECT * EXCLUDE(rn)
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY sku, valid_from ORDER BY source_file DESC) AS rn
        FROM valid_rows
    ) WHERE rn = 1
),
scd_step1 AS (
    SELECT 
        sku,
        unit_cost_sar,
        valid_from,
        LAG(unit_cost_sar) OVER (PARTITION BY sku ORDER BY valid_from) AS prev_cost
    FROM dedup_costs
),
scd_step2 AS (
    SELECT 
        sku,
        unit_cost_sar,
        valid_from,
        LEAD(valid_from) OVER (PARTITION BY sku ORDER BY valid_from) AS valid_to
    FROM scd_step1
    WHERE prev_cost IS NULL OR prev_cost != unit_cost_sar
)
SELECT 
    sku,
    unit_cost_sar,
    valid_from,
    valid_to,
    CASE WHEN valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM scd_step2