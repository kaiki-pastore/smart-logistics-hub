{{ config(materialized='view') }}

select
    vehicle_id,
    vehicle_type,
    capacity_kg,
    status as vehicle_status,
    ingestion_date
from {{ source('erp_source', 'raw_vehicles') }}