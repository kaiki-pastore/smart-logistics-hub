with source as (
    select * from {{ source('postgres_raw', 'raw_inventory') }}
),

renamed_and_casted as (
    select
        cast(order_id as varchar) as order_id,
        
        cast(export_date as date) as export_date,
        
        cast(weight_kg as numeric(10, 2)) as total_weight_kg
        
    from source
)

select * from renamed_and_casted