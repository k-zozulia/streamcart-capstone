{{
    config(
        materialized = 'table',
        tags         = ['incremental']
    )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

-- Keep only the latest record per product_id
ranked as (
    select
        product_id,
        updated_at,
        row_number() over (
            partition by product_id
            order by updated_at desc
        ) as rn
    from orders
),

latest as (
    select product_id
    from ranked
    where rn = 1
),

product_stats as (
    select
        o.product_id,
        count(distinct o.order_id)          as total_orders,
        round(sum(o.total_amount), 2)       as total_revenue,
        round(avg(o.unit_price), 2)         as avg_unit_price,
        min(o.unit_price)                   as min_unit_price,
        max(o.unit_price)                   as max_unit_price,
        min(o.order_date)                   as first_order_date,
        max(o.order_date)                   as last_order_date
    from {{ ref('int_orders_enriched') }} o
    group by o.product_id
)

select
    l.product_id,

    -- Product name placeholder (no product catalog in this dataset)
    -- Power BI can use product_id as the dimension key
    'Product ' || cast(l.product_id as varchar)  as product_name,

    -- Category derived from product_id range (for demo purposes)
    case
        when l.product_id between 1   and 50  then 'Electronics'
        when l.product_id between 51  and 100 then 'Clothing'
        when l.product_id between 101 and 150 then 'Home & Garden'
        when l.product_id between 151 and 200 then 'Sports'
        else 'Other'
    end                                          as product_category,

    -- Metrics
    ps.total_orders,
    ps.total_revenue,
    ps.avg_unit_price,
    ps.min_unit_price,
    ps.max_unit_price,
    ps.first_order_date,
    ps.last_order_date

from latest l
left join product_stats ps on l.product_id = ps.product_id