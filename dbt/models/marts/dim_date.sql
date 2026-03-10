{{
    config(
        materialized = 'table',
        tags         = ['incremental']
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart   = "day",
        start_date = "cast('2020-01-01' as date)",
        end_date   = "dateadd(year, 1, current_date())"
    ) }}
),

final as (
    select
        cast(date_day as date)                          as date_id,
        cast(date_day as date)                          as full_date,

        -- Year / Quarter / Month / Week
        year(date_day)                                  as year,
        quarter(date_day)                               as quarter,
        month(date_day)                                 as month_number,
        monthname(date_day)                             as month_name,
        weekofyear(date_day)                            as week_of_year,

        -- Day
        dayofmonth(date_day)                            as day_of_month,
        dayofweek(date_day)                             as day_of_week_number,
        dayname(date_day)                               as day_of_week_name,

        -- Flags
        case
            when dayofweek(date_day) in (1, 7) then true
            else false
        end                                             as is_weekend,

        -- Start of period shortcuts (useful for DAX time intelligence)
        date_trunc('month',   date_day)                 as first_day_of_month,
        date_trunc('quarter', date_day)                 as first_day_of_quarter,
        date_trunc('year',    date_day)                 as first_day_of_year

    from date_spine
)

select * from final