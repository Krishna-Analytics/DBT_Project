{{ config(materialized='table') }}

SELECT
    user_id,
    created_at,
    deleted_at,
    CASE
        WHEN deleted_at IS NOT NULL THEN 'Churned'
        WHEN created_at >= CURRENT_DATE - INTERVAL '30 day' THEN 'New'
        ELSE 'Active'
    END AS customer_segment
FROM {{ ref('tbl_stg_users') }}
