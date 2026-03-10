with source as (
    select * from {{ source('streamcart_raw', 'orders_curated') }}
),

staged as (
    select
        cast(order_id as integer)           as order_id,
        cast(customer_id as integer)        as customer_id,
        cast(product_id as integer)         as product_id,
        cast(quantity as integer)           as quantity,
        cast(unit_price as float)           as unit_price,
        cast(total_amount as float)         as total_amount,
        lower(trim(status))                 as status,
        cast(created_at as timestamp_tz)    as created_at,
        cast(updated_at as timestamp_tz)    as updated_at,
        cast(order_date as date)            as order_date,
        cast(hour_of_day as integer)        as hour_of_day,
        cast(is_weekend as boolean)         as is_weekend,
        upper(trim(record_type))            as record_type,
        pipeline_run_id                     as pipeline_run_id,
        current_timestamp()                 as _extracted_at
    from source
)

select * from staged