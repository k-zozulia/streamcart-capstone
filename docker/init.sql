-- =============================================================================
-- StreamCart Analytics Platform
-- Source Database DDL + Seed Data
-- =============================================================================

-- Audit table for pipeline runs (used by Airflow task refresh_data_quality_summary)
CREATE TABLE IF NOT EXISTS pipeline_audit (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(100) NOT NULL,
    run_date        DATE NOT NULL,
    curated_rows    INTEGER,
    quarantine_rows INTEGER,
    dbt_test_status VARCHAR(20),   -- 'passed' | 'failed' | 'skipped'
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- =============================================================================
-- Main orders table (CDC watermark = updated_at)
-- =============================================================================
CREATE TABLE IF NOT EXISTS orders (
    order_id      SERIAL          PRIMARY KEY,
    customer_id   INTEGER         NOT NULL,
    product_id    INTEGER         NOT NULL,
    quantity      INTEGER         NOT NULL,
    unit_price    DECIMAL(10, 2)  NOT NULL,
    status        VARCHAR(20)     NOT NULL DEFAULT 'pending',
    created_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- CDC watermark index — keeps extraction fast on large tables
CREATE INDEX IF NOT EXISTS idx_orders_updated_at ON orders(updated_at);

-- Composite index for common filter patterns
CREATE INDEX IF NOT EXISTS idx_orders_status_updated ON orders(status, updated_at);

-- =============================================================================
-- Seed data: 5 000 initial orders spread across 90 days
-- =============================================================================
INSERT INTO orders (customer_id, product_id, quantity, unit_price, status, created_at, updated_at)
SELECT
    -- customer_id: 1–500
    (RANDOM() * 499 + 1)::INTEGER,

    -- product_id: 1–200
    (RANDOM() * 199 + 1)::INTEGER,

    -- quantity: 1–20
    (RANDOM() * 19 + 1)::INTEGER,

    -- unit_price: 5.00–499.99
    ROUND((RANDOM() * 494.99 + 5.00)::NUMERIC, 2),

    -- status: realistic distribution
    (ARRAY['pending','pending','processing','processing','shipped','delivered','delivered','delivered','cancelled'])[
        (RANDOM() * 8 + 1)::INTEGER
    ],

    -- created_at: random moment in last 90 days
    NOW() - (RANDOM() * INTERVAL '90 days'),

    -- updated_at = created_at initially (will be bumped on status updates)
    NOW() - (RANDOM() * INTERVAL '90 days')

FROM generate_series(1, 5000);

-- Make sure updated_at >= created_at (occasionally they can swap due to random)
UPDATE orders SET updated_at = created_at WHERE updated_at < created_at;

-- =============================================================================
-- Trigger: auto-update updated_at on every row change
-- =============================================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_orders_updated_at ON orders;

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW
    EXECUTE FUNCTION set_updated_at();