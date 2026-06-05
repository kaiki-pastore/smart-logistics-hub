{{ config(materialized='view') }}

select
    driver_id,
    name as driver_name,
    license_type,
    status as driver_status,
    ingestion_date
from {{ source('erp_source', 'raw_drivers') }}