WITH orders AS (
    SELECT
        order_id,
        user_id,
        branch_id,
        order_date,
        net_amount_total,
        total_items,
        product_revenue,
        average_order_value
    FROM {{ ref('tbl_metric_orders') }}
),
users AS (
    SELECT
        user_id,
        total_orders,
        total_revenue,
        customer_lifetime_value,
        repeat_purchase_flag,
        churn_flag
    FROM {{ ref('tbl_metric_users') }}
),
products AS (
    SELECT
        product_id,
        product_category,
        total_quantity_sold,
        total_revenue,
        average_revenue_per_order,
        product_popularity_rank
    FROM {{ ref('tbl_metric_product') }}
),
dim_u AS (
    SELECT
        user_id,
        customer_segment
    FROM {{ ref('tbl_dim_users') }}
),
dim_p AS (
    SELECT
        product_id,
        product_name,
        product_category
    FROM {{ ref('tbl_dim_products') }}
),
dim_b AS (
    SELECT
        branch_id
    FROM {{ ref('tbl_dim_orders') }}
),
order_products AS (
    SELECT DISTINCT
        od.order_id,
        od.product_id
    FROM {{ ref('tbl_stg_order_details') }} od
)

SELECT
    o.order_id,
    o.order_date,
    o.user_id,
    u.total_orders,
    u.total_revenue AS user_total_revenue,
    u.customer_lifetime_value,
    u.repeat_purchase_flag,
    u.churn_flag,
    du.customer_segment,
    o.branch_id,
    o.net_amount_total,
    o.total_items,
    o.product_revenue,
    o.average_order_value,
    op.product_id,
    dp.product_name,
    dp.product_category,
    p.total_quantity_sold,
    p.total_revenue AS product_total_revenue,
    p.average_revenue_per_order,
    p.product_popularity_rank
FROM orders o
LEFT JOIN users u 
    ON o.user_id = u.user_id
LEFT JOIN dim_u du 
    ON o.user_id = du.user_id
LEFT JOIN order_products op
    ON o.order_id = op.order_id
LEFT JOIN products p 
    ON op.product_id = p.product_id
LEFT JOIN dim_p dp 
    ON p.product_id = dp.product_id
LEFT JOIN dim_b db 
    ON o.branch_id = db.branch_id
GROUP BY ALL