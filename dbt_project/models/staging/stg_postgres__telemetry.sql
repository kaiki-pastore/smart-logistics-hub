with source as (
    select * from {{ source('postgres_raw', 'raw_telemetry') }}
),

renamed_and_casted as (
    select
        cast(vehicle_id as varchar) as vehicle_id,
        
        cast(timestamp as timestamp) as event_timestamp,
        
        -- Metrics
        cast(latitude as numeric(10, 6)) as location_lat,
        cast(longitude as numeric(10, 6)) as location_lon,
        cast(cargo_temperature as numeric(5, 2)) as cargo_temp_c,
        
        cast(delivery_status as varchar) as delivery_status
        
    from source
)

select * from renamed_and_casted