WITH raw_supplier AS (
    SELECT 
        source_file,
        TRIM(sku) AS sku,
        CAST(valid_from AS DATE) AS valid_from,
        COALESCE(unit_cost, cost_sar) AS raw_cost_str,
        TRY_CAST(
            REPLACE(
                TRANSLATE(COALESCE(unit_cost, cost_sar), '٠١٢٣٤٥٦٧٨٩', '0123456789'), 
                ',', '.'
            ) AS DOUBLE
        ) AS parsed_cost
    FROM {{ source('raw', 'supplier_prices_raw') }}
),
valid_products AS (
    SELECT DISTINCT TRIM(sku) AS sku FROM {{ source('raw', 'products') }}
),
dedup AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY sku, valid_from ORDER BY source_file DESC) AS rn
    FROM raw_supplier
)
SELECT 
    source_file,
    sku,
    valid_from,
    raw_cost_str AS raw_cost,
    CASE 
        WHEN sku NOT IN (SELECT sku FROM valid_products) THEN 'unknown_sku'
        WHEN parsed_cost IS NULL OR parsed_cost <= 0 THEN 'invalid_cost'
    END AS reason
FROM dedup
WHERE rn = 1 
  AND (
      sku NOT IN (SELECT sku FROM valid_products) 
      OR parsed_cost IS NULL 
      OR parsed_cost <= 0
  )