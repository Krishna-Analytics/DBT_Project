{{ config(
    materialized='table'
) }}

with orders as (

    select
        order_id,
        user_id,
        cast(order_date as date) as order_date,
        net_amount_total,
        branch_id
    from {{ ref('tbl_stg_order') }}

)

select
    order_id,
    user_id,
    order_date,
    net_amount_total,
    branch_id,
from orders
