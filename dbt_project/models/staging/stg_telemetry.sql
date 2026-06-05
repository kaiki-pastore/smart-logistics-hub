{{ config(materialized='view') }}

with deduplicated as (
    select
        event_id,
        vehicle_id,
        timestamp as event_timestamp,
        latitude,
        longitude,
        cargo_temp_c,
        speed_kmh,
        ingestion_date,
        row_number() over(partition by event_id order by ingestion_date desc) as rn
    from {{ source('erp_source', 'raw_telemetry') }}
)
select
    t.event_id,
    t.vehicle_id,
    t.event_timestamp,
    t.latitude,
    t.longitude,
    t.cargo_temp_c,
    t.speed_kmh,
    t.ingestion_date
from deduplicated t
inner join {{ source('erp_source', 'raw_vehicles') }} v
    on t.vehicle_id = v.vehicle_id
where t.rn = 1