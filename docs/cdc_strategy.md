# CDC Strategy — StreamCart Analytics Platform

## 1. Overview

StreamCart uses **Watermark-Based CDC** (Change Data Capture) to detect inserts and updates
in the PostgreSQL source database and land them in S3 for downstream processing.

The watermark column is `updated_at` — a `TIMESTAMP WITH TIME ZONE` column that is
automatically bumped on every row change via a PostgreSQL trigger (`trg_orders_updated_at`).

---

## 2. How the Watermark Works

### Initial State
When the pipeline runs for the first time, the watermark is set to `2020-01-01 00:00:00+00:00`
(stored in Airflow Variable `last_cdc_watermark`). This guarantees all historical rows are
captured in the first FULL load.

### Each Incremental Run

```
1. Read last_cdc_watermark from Airflow Variable
   e.g. "2024-03-14 22:00:00+00:00"

2. Run query:
   SELECT * FROM orders
   WHERE updated_at > '2024-03-14 22:00:00+00:00'
   ORDER BY updated_at

3. Write results to S3
   s3://.../raw/orders/date=2024-03-15/batch_*.json

4. Update watermark to max(updated_at) of the batch
   new value: "2024-03-15 14:32:00.123456+00:00"
```

### Critical Rule: Watermark Updates Only After Successful S3 Write

If the S3 write fails, the watermark is NOT updated. On the next run, the same rows
are re-extracted and re-landed. This guarantees at-least-once delivery.

Downstream deduplication (Glue job + dbt fact_orders with unique_key = order_id) handles duplicates.

---

## 3. Full Load vs Incremental Mode

Controlled by Airflow Variable `pipeline_mode`.

| Mode | When to Use | What Happens |
|------|-------------|--------------|
| full | First run, disaster recovery, schema change | Clears raw S3 prefix, re-extracts ALL rows |
| incremental | Every scheduled run (default) | Extracts only rows where updated_at > watermark |

---

## 4. Late-Arriving Data

### The Problem

A row might arrive with an `updated_at` timestamp from 2 hours ago due to replica lag
or batch backfill jobs.

Example:
- Current time: 2024-03-15 14:00:00 UTC
- Last watermark: 2024-03-15 13:00:00 UTC
- Late row arrives: updated_at = 2024-03-15 11:30:00 UTC → MISSED

### Production Solution (Description Only — Not Implemented)

**Lookback Window Approach:**

Instead of WHERE updated_at > watermark, use a buffer:

  WHERE updated_at > (watermark - INTERVAL '2 hours')

This re-processes the last 2 hours of data on every run.
Combined with unique_key = order_id in dbt, duplicates are handled automatically.

Trade-offs:
- Pro: catches late arrivals within the lookback window
- Con: slightly larger batches per run
- Recommended lookback: 2x the maximum observed replica lag

**Alternative — WAL-Based CDC:**

Use Debezium with PostgreSQL logical replication. Debezium streams every row change
the moment it is committed to the WAL, eliminating the late-arrival problem entirely.

---

## 5. Idempotency

The pipeline is safe to re-run at any stage:

- S3 writes use timestamped file names — re-running creates a new file, not overwrite
- Glue job detects INSERT vs UPDATE by comparing with existing curated partition
- dbt fact_orders uses incremental_strategy = merge with unique_key = order_id

---

## 6. S3 Landing Zone Layout

```
s3://karina-ecommerce-lakehouse/
└── ecommerce/
    └── raw/
        └── orders/
            ├── date=2024-03-13/
            │   └── batch_20240313T060000Z.json
            ├── date=2024-03-14/
            │   └── batch_20240314T060000Z.json
            ├── date=2024-03-15/
            │   ├── batch_20240315T060000Z.json
            │   └── batch_20240315T143200Z.json
            └── full_load/
                └── full_20240315T143200Z.json
```

The date=YYYY-MM-DD partition format is Hive-compatible — AWS Glue and Athena
discover these partitions automatically without a crawler.

---

## 7. Monitoring

| Metric | Where | Action |
|--------|-------|--------|
| rows_extracted = 0 | Airflow task log | ShortCircuitOperator skips downstream |
| S3 write failure | Airflow + CloudWatch | notify_on_failure callback fires |
| Source freshness | dbt source freshness | warn_after: 25h, error_after: 49h |
