{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

select
    order_id,
    product_name,
    weight_kg,
    destination_lat,
    destination_lon,
    order_status,
    order_created_at,
    ingestion_date
from {{ ref('stg_orders') }}

{% if is_incremental() %}
  where ingestion_date > (select coalesce(max(destiny.ingestion_date), '1900-01-01'::timestamp) from {{ this }} as destiny)
{% endif %}