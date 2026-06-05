{{ config(materialized='table') }}

select
    vehicle_id,
    vehicle_type,
    capacity_kg,
    vehicle_status
from {{ ref('stg_vehicles') }}