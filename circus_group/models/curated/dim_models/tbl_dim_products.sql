{{ config(materialized='table') }}

SELECT
    product_id,
    product_name,
    product_category
FROM {{ ref('tbl_stg_products') }}
