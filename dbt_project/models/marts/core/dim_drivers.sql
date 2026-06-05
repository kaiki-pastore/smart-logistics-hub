{{ config(materialized='table') }}

select
    driver_id,
    driver_name,
    license_type,
    driver_status
from {{ ref('stg_drivers') }}
where driver_status = 'AVAILABLE'