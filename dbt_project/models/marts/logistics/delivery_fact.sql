with telemetry as (
    select * from {{ ref('stg_postgres__telemetry') }}
)
select
    event_timestamp,
    vehicle_id,
    location_lat,
    location_lon,
    cargo_temp_c,
    delivery_status
from telemetry