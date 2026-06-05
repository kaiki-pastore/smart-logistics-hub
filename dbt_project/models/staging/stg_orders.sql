{{ config(materialized='view') }}

with deduplicated as (
    select
        order_id,
        product_name,
        weight_kg,
        destination_lat,
        destination_lon,
        status as order_status,
        created_at as order_created_at,
        ingestion_date,
        row_number() over(partition by order_id order by ingestion_date desc) as rn
    from {{ source('erp_source', 'raw_orders') }}
)
select
    order_id,
    product_name,
    weight_kg,
    destination_lat,
    destination_lon,
    order_status,
    order_created_at,
    ingestion_date
from deduplicated
where rn = 1