-- =============================================================================
-- Singular test: total_amount must equal quantity * unit_price
-- Returns rows where the calculation is wrong (should return 0 rows)
-- =============================================================================

select
    order_id,
    quantity,
    unit_price,
    total_amount,
    round(quantity * unit_price, 2) as expected_total_amount
from {{ ref('fact_orders') }}
where
    -- Allow for floating point rounding tolerance of 0.01
    abs(total_amount - round(quantity * unit_price, 2)) > 0.01