WITH product AS (
    SELECT
        C1 AS ID, 
        C2 AS NAME, 
        C3 AS CATEGORY, 
    FROM
        CIRCUS_OPS_REPORTING_LAYER.RAW.PRODUCTS
    where c1 != 'id'   -- exclude the extra header row
)
SELECT
    ID AS product_id,
    NAME AS product_name,
    CATEGORY AS product_category,
FROM product