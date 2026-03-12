-- =============================================================================
-- Mart layer: one row per order, analytics-ready
-- =============================================================================

{{
    config(
        materialized = 'incremental',
        unique_key   = 'order_id',
        on_schema_change = 'sync_all_columns',
        tags         = ['incremental']
    )
}}

with quality as (
    select * from {{ ref('int_orders_quality_flags') }}

    {% if is_incremental() %}
        where _extracted_at > (
            select max(_extracted_at) from {{ this }}
        )
    {% endif %}
),

-- Deduplicate: keep latest updated_at per order_id
-- Handles cases where same order_id appears multiple times in a batch
-- (e.g. duplicate rows inserted by seed_updates.py)
deduped as (
    select *
    from quality
    qualify row_number() over (
        partition by order_id
        order by updated_at desc
    ) = 1
),

final as (
    select
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        total_amount,
        status,
        order_date,
        hour_of_day,
        is_weekend,
        day_of_week,
        order_month,
        order_year,
        fulfillment_days,
        record_type,
        pipeline_run_id,
        case when record_type = 'INSERT' then true else false end   as is_insert,
        case when record_type = 'UPDATE' then true else false end   as is_update,
        created_at,
        updated_at,
        _extracted_at,
        _extracted_at                                               as first_seen_at,
        _extracted_at                                               as last_updated_at,
        is_valid_quantity,
        is_valid_price,
        is_duplicate,
        is_valid_overall,
        rejection_reason

    from deduped
)

select * from final