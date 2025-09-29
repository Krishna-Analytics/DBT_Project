{{ config(
    materialized='table'
) }}

with order_details as (

    select
        id as order_detail_id,
        order_id,
        product_id,
        quantity,
        cast(order_date as date) as order_date,
        net_price
    from {{ ref('tbl_stg_order_details') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['order_detail_id']) }} as order_detail_sk,
    order_detail_id,
    order_id,
    product_id,
    quantity,
    order_date,
    net_price,
    (quantity * net_price) as line_total
from order_details
