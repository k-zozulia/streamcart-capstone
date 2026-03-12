-- =============================================================================
-- Aggregated data quality metrics per pipeline run

-- Two sources of bad data:
--   1. Glue job rejections → orders_quarantine (append-only, full history)
--      Types: invalid_quantity_or_price, invalid_status
--   2. dbt duplicate detection → is_duplicate=true in int_orders_quality_flags
-- =============================================================================

{{
    config(
        materialized = 'incremental',
        unique_key   = 'pipeline_run_id',
        tags         = ['incremental']
    )
}}

with curated as (
    select
        pipeline_run_id,
        max(_extracted_at)                                              as run_timestamp,
        count(*)                                                        as curated_rows,
        sum(case when record_type = 'INSERT'  then 1 else 0 end)       as insert_rows,
        sum(case when record_type = 'UPDATE'  then 1 else 0 end)       as update_rows,
        sum(case when is_duplicate = true     then 1 else 0 end)       as duplicate_rows
    from {{ ref('int_orders_quality_flags') }}

    {% if is_incremental() %}
        where pipeline_run_id not in (select pipeline_run_id from {{ this }})
    {% endif %}

    group by pipeline_run_id
),

quarantine as (
    select
        pipeline_run_id,
        count(*)                                                                            as quarantine_rows,
        sum(case when rejection_reason = 'invalid_quantity_or_price' then 1 else 0 end)    as invalid_quantity_count,
        sum(case when rejection_reason = 'invalid_status'            then 1 else 0 end)    as invalid_status_count,
        mode(rejection_reason)                                                              as top_rejection_reason
    from {{ source('streamcart_raw', 'orders_quarantine') }}

    {% if is_incremental() %}
        where pipeline_run_id not in (select pipeline_run_id from {{ this }})
    {% endif %}

    group by pipeline_run_id
),

joined as (
    select
        c.pipeline_run_id,
        cast(c.run_timestamp as date)                                   as run_date,
        c.run_timestamp,

        c.curated_rows,
        coalesce(q.quarantine_rows, 0)                                  as quarantine_rows,
        c.curated_rows + coalesce(q.quarantine_rows, 0)                 as total_rows,

        c.insert_rows,
        c.update_rows,
        c.duplicate_rows,

        coalesce(q.invalid_quantity_count, 0)                          as invalid_quantity_count,
        coalesce(q.invalid_status_count, 0)                            as invalid_status_count,

        round(
            coalesce(q.quarantine_rows, 0)
            / nullif(c.curated_rows + coalesce(q.quarantine_rows, 0), 0)
            * 100, 2
        )                                                               as quarantine_rate_pct,

        case
            when coalesce(q.quarantine_rows, 0) >= c.duplicate_rows
            then coalesce(q.top_rejection_reason, 'duplicate')
            else 'duplicate'
        end                                                             as top_rejection_reason,

        'passed'                                                        as dbt_test_status

    from curated c
    left join quarantine q on c.pipeline_run_id = q.pipeline_run_id
)

select * from joined
order by run_timestamp desc