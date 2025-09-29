{{ config(
    materialized='incremental',
    unique_key='user_id',
    incremental_strategy='merge',
    partition_by={"field": "created_at", "data_type": "date"},
) }}

WITH users AS (
    SELECT
        C1 AS ID, 
        C2 AS created_at, 
        C3 AS deleted_at, 
        
    FROM {{ source('OPS_REPORTING_LAYER_RAW', 'USERS') }}
    where c1 != 'id'   -- exclude the extra header row
)
SELECT
    ID AS user_id,
    created_at AS created_at,
    deleted_at AS deleted_at,
FROM users

{% if is_incremental() %}
where created_at >= (select max(order_date) from {{ this }})
{% endif %}
	
