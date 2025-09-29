{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge',
    partition_by={"field": "created_at", "data_type": "date"},
) }}

WITH product_sales AS (
    SELECT
        p.product_id,
        p.product_category,
        SUM(od.quantity) AS total_quantity_sold,
        SUM(od.net_price) AS total_revenue,
        COUNT(DISTINCT od.order_id) AS distinct_orders
    FROM {{ ref('tbl_stg_products') }} p
    LEFT JOIN {{ ref('tbl_stg_order_details') }} od
      ON p.product_id = od.product_id
    GROUP BY p.product_id, p.product_category
)

SELECT
    product_id,
    product_category,
    total_quantity_sold,
    total_revenue,
    total_revenue / NULLIF(distinct_orders, 0) AS average_revenue_per_order,
    RANK() OVER (ORDER BY total_quantity_sold DESC) AS product_popularity_rank
FROM product_sales


{% if is_incremental() %}
where created_at >= (select max(order_date) from {{ this }})
{% endif %}
	