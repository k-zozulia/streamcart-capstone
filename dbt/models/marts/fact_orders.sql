-- =============================================================================
-- fact_orders.sql
-- Mart layer: one row per order, analytics-ready
--
-- Incremental strategy:
--   unique_key = order_id → Snowflake MERGE on order_id
--   On match (UPDATE): overwrite all columns with latest values
--   On insert (new order_id): append new row
--
--   Filter: on incremental runs, only process rows where _extracted_at
--   is greater than the max _extracted_at already in the table.
--   This ensures we never reprocess rows already loaded, while still
--   capturing status updates to existing orders.
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
        -- Only process new rows since last run
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
        -- Primary key
        order_id,

        -- Dimensions (foreign keys for star schema)
        customer_id,
        product_id,

        -- Order attributes
        quantity,
        unit_price,
        total_amount,
        status,

        -- Date dimensions
        order_date,
        hour_of_day,
        is_weekend,
        day_of_week,
        order_month,
        order_year,
        fulfillment_days,

        -- CDC metadata
        record_type,
        pipeline_run_id,

        -- CDC flags for Power BI Pipeline Health page
        case when record_type = 'INSERT' then true else false end   as is_insert,
        case when record_type = 'UPDATE' then true else false end   as is_update,

        -- Audit timestamps
        created_at,
        updated_at,
        _extracted_at,

        -- first_seen_at and last_updated_at for SCD tracking
        -- On incremental merge: first_seen_at keeps original value,
        -- last_updated_at always updates to latest
        _extracted_at                                               as first_seen_at,
        _extracted_at                                               as last_updated_at,

        -- Data quality flags (from intermediate layer)
        is_valid_quantity,
        is_valid_price,
        is_duplicate,
        is_valid_overall,
        rejection_reason

    from deduped
)

select * from final