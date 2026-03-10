with enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

with_flags as (
    select
        *,

        -- Quantity must be positive
        case
            when quantity > 0 then true
            else false
        end                             as is_valid_quantity,

        -- Price must be non-negative
        case
            when unit_price >= 0 then true
            else false
        end                             as is_valid_price,

        -- Duplicate detection:
        -- count how many rows share the same business key combo
        -- (customer_id, product_id, quantity, unit_price, status)
        -- If count > 1 → flag all of them as duplicates
        case
            when count(*) over (
                partition by
                    customer_id,
                    product_id,
                    quantity,
                    unit_price,
                    status
            ) > 1
            then true
            else false
        end                             as is_duplicate

    from enriched
),

with_overall as (
    select
        *,

        -- Overall quality flag — false if ANY individual flag fails
        case
            when is_valid_quantity
             and is_valid_price
             and not is_duplicate
            then true
            else false
        end                             as is_valid_overall,

        -- Rejection reason for quarantine mart
        case
            when not is_valid_quantity  then 'invalid_quantity'
            when not is_valid_price     then 'invalid_price'
            when is_duplicate           then 'duplicate'
            else null
        end                             as rejection_reason

    from with_flags
)

select * from with_overall