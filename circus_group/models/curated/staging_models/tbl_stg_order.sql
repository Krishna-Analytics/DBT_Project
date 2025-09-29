{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    partition_by={"field": "order_date", "data_type": "date"},
    cluster_by=["user_id", "branch_id"]
) }}


WITH orders AS (
    SELECT
            ID, 
            USER_ID, 
            BRANCH_ID, 
            ORDER_DATE, 
            NET_AMOUNT_TOTAL
        FROM {{ source('OPS_REPORTING_LAYER_RAW', 'ORDERS') }}
)
SELECT
    ID AS order_id,
    USER_ID AS user_id,
    BRANCH_ID AS branch_id,
    ORDER_DATE AS order_date,
    NET_AMOUNT_TOTAL AS net_amount_total
FROM orders

{% if is_incremental() %}
where order_date >= (select max(order_date) from {{ this }})
{% endif %}