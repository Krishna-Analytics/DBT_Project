{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    partition_by={"field": "created_at", "data_type": "date"},
) }}

WITH user_orders AS (
    SELECT
        o.user_id,
        COUNT(o.order_id) AS total_orders,
        SUM(o.net_amount_total) AS total_revenue,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date
    FROM {{ ref('tbl_stg_order') }} o
    GROUP BY o.user_id
)

SELECT
    u.user_id,
    u.created_at,
    u.deleted_at,
    total_orders,
    total_revenue,
    total_revenue / NULLIF(total_orders, 0) AS average_order_value,
    CASE WHEN total_orders > 1 THEN TRUE ELSE FALSE END AS repeat_purchase_flag,
    total_revenue AS customer_lifetime_value,
    CASE WHEN u.deleted_at IS NOT NULL THEN TRUE ELSE FALSE END AS churn_flag
FROM {{ ref('tbl_stg_users') }} u
LEFT JOIN user_orders o
  ON u.user_id = o.user_id


{% if is_incremental() %}
where created_at >= (select max(order_date) from {{ this }})
{% endif %}