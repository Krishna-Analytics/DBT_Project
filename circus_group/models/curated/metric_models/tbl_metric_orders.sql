{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    partition_by={"field": "created_at", "data_type": "date"},
) }}

WITH order_base AS (
    SELECT
        o.order_id,
        o.user_id,
        o.branch_id,
        o.order_date,
        o.net_amount_total,
        SUM(od.quantity) AS total_items,
        SUM(od.net_price) AS product_revenue
    FROM {{ ref('tbl_stg_order') }} o
    LEFT JOIN {{ ref('tbl_stg_order_details') }} od
      ON o.order_id = od.order_id
    GROUP BY o.order_id, o.user_id, o.branch_id, o.order_date, o.net_amount_total
)

SELECT
    order_id,
    user_id,
    branch_id,
    order_date,
    net_amount_total,
    total_items,
    product_revenue,
    net_amount_total / NULLIF(total_items, 0) AS average_order_value
FROM order_base

{% if is_incremental() %}
where created_at >= (select max(order_date) from {{ this }})
{% endif %}
	