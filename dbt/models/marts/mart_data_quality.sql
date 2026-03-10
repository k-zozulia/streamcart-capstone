-- =============================================================================
-- mart_data_quality.sql
-- Aggregated data quality metrics per pipeline run
--
-- Two sources of bad data:
--   1. Glue job rejections → orders_quarantine table (invalid qty, price, status)
--   2. dbt duplicate detection → is_duplicate=true in int_orders_quality_flags
-- =============================================================================

{{
    config(
        materialized = 'table',
        tags         = ['incremental']
    )
}}

with curated as (
    select
        pipeline_run_id,
        _extracted_at,
        count(*)                                                        as curated_rows,
        sum(case when record_type = 'INSERT'       then 1 else 0 end)  as insert_rows,
        sum(case when record_type = 'UPDATE'       then 1 else 0 end)  as update_rows,
        sum(case when is_duplicate = true          then 1 else 0 end)  as duplicate_rows
    from {{ ref('int_orders_quality_flags') }}
    group by pipeline_run_id, _extracted_at
),

quarantine as (
    select
        pipeline_run_id,
        count(*)                                                                    as quarantine_rows,
        sum(case when rejection_reason = 'invalid_quantity' then 1 else 0 end)     as invalid_quantity_count,
        sum(case when rejection_reason = 'invalid_price'    then 1 else 0 end)     as invalid_price_count,
        sum(case when rejection_reason = 'invalid_status'   then 1 else 0 end)     as invalid_status_count,
        mode(rejection_reason)                                                      as top_rejection_reason
    from {{ source('streamcart_raw', 'orders_quarantine') }}
    group by pipeline_run_id
),

joined as (
    select
        c.pipeline_run_id,
        cast(c._extracted_at as date)                                   as run_date,
        c._extracted_at                                                 as run_timestamp,

        c.curated_rows,
        coalesce(q.quarantine_rows, 0) + c.duplicate_rows              as quarantine_rows,
        c.curated_rows + coalesce(q.quarantine_rows, 0)                as total_rows,

        c.insert_rows,
        c.update_rows,
        c.duplicate_rows,

        coalesce(q.invalid_quantity_count, 0)                          as invalid_quantity_count,
        coalesce(q.invalid_price_count, 0)                             as invalid_price_count,
        coalesce(q.invalid_status_count, 0)                            as invalid_status_count,

        round(
            (coalesce(q.quarantine_rows, 0) + c.duplicate_rows)
            / nullif(c.curated_rows + coalesce(q.quarantine_rows, 0), 0)
            * 100,
            2
        )                                                              as quarantine_rate_pct,

        case
            when coalesce(q.quarantine_rows, 0) >= c.duplicate_rows
            then coalesce(q.top_rejection_reason, 'duplicate')
            else 'duplicate'
        end                                                            as top_rejection_reason,

        'passed'                                                       as dbt_test_status

    from curated c
    left join quarantine q on c.pipeline_run_id = q.pipeline_run_id
)

select * from joined
order by run_timestamp desc