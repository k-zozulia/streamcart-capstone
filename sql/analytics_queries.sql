-- ============================================================
-- StreamCart Analytics Platform
-- Analytical Queries
-- Target: Snowflake (STREAMCART_DB.STREAMCART_MARTS)
-- ============================================================

-- Query 1: Daily Revenue Trend (last 30 days)
-- Shows revenue trend over time to identify growth or decline patterns
-- Used in Power BI: Sales Overview — line chart
-- ============================================================
SELECT
    d.date_actual                           AS order_date,
    d.day_of_week_name                      AS day_name,
    COUNT(fo.order_id)                      AS total_orders,
    SUM(fo.total_amount)                    AS daily_revenue,
    AVG(fo.total_amount)                    AS avg_order_value,
    SUM(SUM(fo.total_amount)) OVER (
        ORDER BY d.date_actual
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )                                       AS revenue_7d_rolling
FROM STREAMCART_MARTS.fact_orders fo
JOIN STREAMCART_MARTS.dim_date d
    ON fo.order_date = d.date_actual
WHERE d.date_actual >= DATEADD('day', -30, CURRENT_DATE)
  AND fo.status != 'cancelled'
GROUP BY 1, 2
ORDER BY 1;


-- Query 2: Top 10 Products by Revenue
-- Identifies best-performing products for inventory and marketing decisions
-- Used in Power BI: Product Performance page
-- ============================================================
SELECT
    fo.product_id,
    dp.product_category,
    COUNT(DISTINCT fo.order_id)             AS total_orders,
    SUM(fo.quantity)                        AS total_units_sold,
    SUM(fo.total_amount)                    AS total_revenue,
    AVG(fo.unit_price)                      AS avg_unit_price,
    ROUND(
        SUM(fo.total_amount) /
        SUM(SUM(fo.total_amount)) OVER () * 100,
        2
    )                                       AS revenue_share_pct
FROM STREAMCART_MARTS.fact_orders fo
JOIN STREAMCART_MARTS.dim_product dp
    ON fo.product_id = dp.product_id
WHERE fo.status NOT IN ('cancelled')
GROUP BY 1, 2
ORDER BY total_revenue DESC
LIMIT 10;


-- Query 3: Order Status Funnel
-- Tracks how orders move through the fulfillment pipeline
-- Useful for identifying bottlenecks
-- ============================================================
SELECT
    status,
    COUNT(order_id)                         AS order_count,
    SUM(total_amount)                       AS total_revenue,
    ROUND(
        COUNT(order_id) * 100.0 /
        SUM(COUNT(order_id)) OVER (),
        2
    )                                       AS pct_of_total,
    AVG(DATEDIFF('hour', created_at, updated_at))
                                            AS avg_hours_in_status
FROM STREAMCART_MARTS.fact_orders
GROUP BY 1
ORDER BY
    CASE status
        WHEN 'pending'    THEN 1
        WHEN 'processing' THEN 2
        WHEN 'shipped'    THEN 3
        WHEN 'delivered'  THEN 4
        WHEN 'cancelled'  THEN 5
    END;


-- Query 4: CDC Audit — INSERT vs UPDATE ratio per pipeline run
-- Shows data change patterns: how many records are new vs updated each run
-- Used in Power BI: Pipeline Health page
-- ============================================================
SELECT
    fo.pipeline_run_id,
    MIN(fo.first_seen_at)                   AS run_date,
    COUNT(fo.order_id)                      AS total_records,
    SUM(CASE WHEN fo.is_insert THEN 1 ELSE 0 END)
                                            AS insert_count,
    SUM(CASE WHEN fo.is_update THEN 1 ELSE 0 END)
                                            AS update_count,
    ROUND(
        SUM(CASE WHEN fo.is_update THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(COUNT(fo.order_id), 0),
        2
    )                                       AS update_rate_pct,
    mdq.quarantine_rows,
    mdq.quarantine_rate_pct,
    mdq.dbt_test_status
FROM STREAMCART_MARTS.fact_orders fo
LEFT JOIN STREAMCART_MARTS.mart_data_quality mdq
    ON fo.pipeline_run_id = mdq.pipeline_run_id
GROUP BY fo.pipeline_run_id, mdq.quarantine_rows, mdq.quarantine_rate_pct, mdq.dbt_test_status
ORDER BY run_date DESC;


-- Query 5: Data Quality Breakdown by Rejection Reason
-- Shows which data quality issues are most common
-- Helps data engineers prioritize fixes at the source
-- ============================================================
SELECT
    rejection_reason,
    COUNT(*)                                AS rejected_rows,
    ROUND(
        COUNT(*) * 100.0 /
        SUM(COUNT(*)) OVER (),
        2
    )                                       AS pct_of_quarantine,
    MIN(created_at)                         AS first_seen,
    MAX(created_at)                         AS last_seen
FROM STREAMCART_DB.RAW.orders_quarantine
GROUP BY 1
ORDER BY rejected_rows DESC;


-- Query 6: Revenue by Product Category — Week over Week
-- Compares this week's category revenue vs last week
-- Used for trend detection in Sales Overview
-- ============================================================
WITH weekly_revenue AS (
    SELECT
        dp.product_category,
        DATE_TRUNC('week', fo.order_date)   AS week_start,
        SUM(fo.total_amount)                AS weekly_revenue
    FROM STREAMCART_MARTS.fact_orders fo
    JOIN STREAMCART_MARTS.dim_product dp
        ON fo.product_id = dp.product_id
    WHERE fo.status != 'cancelled'
      AND fo.order_date >= DATEADD('week', -4, CURRENT_DATE)
    GROUP BY 1, 2
),
wow AS (
    SELECT
        product_category,
        week_start,
        weekly_revenue,
        LAG(weekly_revenue) OVER (
            PARTITION BY product_category
            ORDER BY week_start
        )                                   AS prev_week_revenue
    FROM weekly_revenue
)
SELECT
    product_category,
    week_start,
    weekly_revenue,
    prev_week_revenue,
    ROUND(
        (weekly_revenue - prev_week_revenue) * 100.0 /
        NULLIF(prev_week_revenue, 0),
        2
    )                                       AS wow_change_pct
FROM wow
ORDER BY week_start DESC, weekly_revenue DESC;


-- Query 7: Customer Cohort — Average Order Value by Order Count
-- Segments customers by purchase frequency
-- Reveals whether repeat customers have higher order values
-- ============================================================
WITH customer_stats AS (
    SELECT
        customer_id,
        COUNT(order_id)                     AS total_orders,
        SUM(total_amount)                   AS lifetime_value,
        AVG(total_amount)                   AS avg_order_value,
        MIN(order_date)                     AS first_order_date,
        MAX(order_date)                     AS last_order_date
    FROM STREAMCART_MARTS.fact_orders
    WHERE status != 'cancelled'
    GROUP BY 1
)
SELECT
    CASE
        WHEN total_orders = 1  THEN '1 order (one-time)'
        WHEN total_orders <= 3 THEN '2-3 orders (occasional)'
        WHEN total_orders <= 7 THEN '4-7 orders (regular)'
        ELSE '8+ orders (loyal)'
    END                                     AS customer_segment,
    COUNT(customer_id)                      AS customer_count,
    ROUND(AVG(avg_order_value), 2)          AS avg_order_value,
    ROUND(AVG(lifetime_value), 2)           AS avg_lifetime_value,
    ROUND(AVG(total_orders), 1)             AS avg_orders_per_customer
FROM customer_stats
GROUP BY 1
ORDER BY avg_lifetime_value DESC;
