{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge',
    partition_by={"field": "order_date", "data_type": "date"},
    cluster_by=["product_id", "order_id"]
) }}

with source_data as (
    select
        id,
        order_id,
        product_id,
        quantity,
        order_date,
        net_price
   FROM {{ source('OPS_REPORTING_LAYER_RAW', 'ORDER_DETAILS') }}
)

select
    id as order_detail_id,
    order_id,
    product_id,
    quantity,
    order_date,
    net_price
from source_data

{% if is_incremental() %}
where order_date >= (select max(order_date) from {{ this }})
{% endif %}
