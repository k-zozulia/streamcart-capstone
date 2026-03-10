with staged as (
    select * from {{ ref('stg_orders') }}
),

enriched as (
    select
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        status,
        created_at,
        updated_at,
        order_date,
        hour_of_day,
        is_weekend,
        record_type,
        pipeline_run_id,
        _extracted_at,

        -- Recompute total_amount in dbt layer (single source of truth)
        round(quantity * unit_price, 2)     as total_amount,

        -- Fulfillment days: days from created_at sto shipped/delivered
        -- NULL if order not yet shipped
        case
            when status in ('shipped', 'delivered')
            then datediff(
                'day',
                cast(created_at as date),
                cast(updated_at as date)
            )
            else null
        end                                 as fulfillment_days,

        -- Day of week name for reporting
        dayname(order_date)                 as day_of_week,

        -- Month and year for aggregations
        date_trunc('month', order_date)     as order_month,
        year(order_date)                    as order_year

    from staged
)

select * from enriched