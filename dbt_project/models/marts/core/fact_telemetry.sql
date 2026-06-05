{{ config(materialized='incremental', unique_key='event_id') }}

select
    t.event_id,
    t.vehicle_id,
    t.event_timestamp,
    t.latitude,
    t.longitude,
    t.cargo_temp_c,
    t.speed_kmh,
    case 
        when t.cargo_temp_c > 8.0 then 'CRITICAL_HIGH'
        when t.cargo_temp_c < 2.0 then 'CRITICAL_LOW'
        else 'NORMAL'
    end as temperature_alert_status
from {{ ref('stg_telemetry') }} t