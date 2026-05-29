with source as (
    select * from {{ source('postgres_raw', 'raw_fleet') }}
)

select
    cast(vehicle_id as varchar) as vehicle_id,
    cast(capacity_kg as integer) as capacity_kg,
    cast(ingestion_date as timestamp) as loaded_at
from source