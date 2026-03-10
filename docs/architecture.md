# StreamCart Analytics Platform — Architecture

## Overview

StreamCart is a production-grade e-commerce data pipeline that detects changes from a PostgreSQL
source database and delivers analytics-ready data to Power BI via a multi-layer data lakehouse.

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Apache Airflow (Docker)                         │
│  check_source → extract_cdc → validate → glue → copy_snowflake →       │
│  run_dbt → refresh_audit                                                │
└────────────┬────────────────────────┬───────────────────────────────────┘
             │                        │
             ▼                        ▼
┌────────────────────┐    ┌───────────────────────────────────────────────┐
│  PostgreSQL         │    │                   AWS                         │
│  (Docker)           │    │                                               │
│                     │    │  S3 Raw Zone                                  │
│  orders table       │───▶│  ecommerce/raw/orders/date=YYYY-MM-DD/       │
│  pipeline_audit     │    │  ecommerce/raw/orders/full_load/              │
│                     │    │         │                                     │
└────────────────────┘    │         ▼                                     │
                           │  AWS Glue (PySpark)                           │
                           │  raw_to_curated.py                            │
                           │  • Filters dirty rows → quarantine            │
                           │  • Detects INSERT vs UPDATE                   │
                           │  • Partitions by order_date                   │
                           │         │                                     │
                           │         ▼                                     │
                           │  S3 Curated Zone                              │
                           │  ecommerce/curated/orders/                    │
                           │  ecommerce/quarantine/orders/                 │
                           └───────────────────┬───────────────────────────┘
                                               │
                                               ▼
                           ┌───────────────────────────────────────────────┐
                           │              Snowflake                         │
                           │  STREAMCART_DB.RAW.orders_curated             │
                           │  STREAMCART_DB.RAW.orders_quarantine          │
                           │         │                                     │
                           │         ▼                                     │
                           │  dbt Transformations                          │
                           │  staging → intermediate → marts               │
                           │         │                                     │
                           │         ▼                                     │
                           │  STREAMCART_STAGING  (views)                  │
                           │  STREAMCART_MARTS    (tables)                 │
                           └───────────────────┬───────────────────────────┘
                                               │
                                               ▼
                           ┌───────────────────────────────────────────────┐
                           │              Power BI                          │
                           │  Sales Overview / Product Performance /        │
                           │  Pipeline Health                               │
                           └───────────────────────────────────────────────┘
```

## Component Descriptions

### PostgreSQL (Docker)
- Simulates a live OLTP e-commerce database
- `orders` table: order_id, customer_id, product_id, quantity, unit_price, status, created_at, updated_at
- `pipeline_audit` table: one row per DAG run with curated/quarantine counts and dbt status
- `updated_at` column is the CDC watermark — indexed for fast extraction

### CDC Extractor (`scripts/cdc_extractor.py`)
- Watermark-based change detection using `updated_at > last_cdc_watermark`
- Two modes: FULL (re-extract all rows) and INCREMENTAL (only changes since last run)
- Writes NDJSON to S3 under `ecommerce/raw/orders/date=YYYY-MM-DD/batch_<timestamp>.json`
- Watermark stored in Airflow Variable `last_cdc_watermark` — updated only after successful S3 write

### Apache Airflow (Docker Compose)
- Orchestrates the full pipeline on a daily schedule
- 7 tasks: check_source_availability → extract_cdc → validate_extract → trigger_glue_job →
  copy_into_snowflake → run_dbt_models → refresh_data_quality_summary
- ShortCircuitOperator skips downstream tasks if 0 rows extracted
- notify_on_failure callback logs failures on all tasks

### AWS S3 — Data Lake Layout
```
s3://karina-ecommerce-lakehouse/
├── ecommerce/
│   ├── raw/
│   │   └── orders/
│   │       ├── date=YYYY-MM-DD/     ← incremental CDC batches
│   │       └── full_load/           ← full extraction snapshots
│   ├── curated/
│   │   └── orders/                  ← clean Parquet, partitioned by order_date
│   ├── quarantine/
│   │   └── orders/                  ← rejected rows with rejection_reason
│   └── audit/                       ← pipeline run summaries
└── glue-scripts/
    └── raw_to_curated.py
```

### AWS Glue (`glue_jobs/raw_to_curated.py`)
- PySpark job with 2 DPUs (minimum cost configuration)
- Accepts `--S3_INPUT_PATH` and `--PIPELINE_RUN_ID` as job parameters from Airflow
- Quarantine logic: missing order_id, negative quantity/price, invalid status
- INSERT vs UPDATE detection by comparing incoming order_ids with existing curated partition
- Adds `record_type` (INSERT/UPDATE) and `pipeline_run_id` columns
- Logs PIPELINE_SUMMARY to CloudWatch: input, insert, update, quarantine counts

### Snowflake
- Database: `STREAMCART_DB`
- Schemas: `RAW` (source tables), `STREAMCART_STAGING` (dbt views), `STREAMCART_MARTS` (dbt tables)
- Data loaded via `COPY INTO` from S3 External Stage using Storage Integration
- TRUNCATE + COPY INTO strategy ensures Snowflake always mirrors S3 curated exactly

### dbt (dbt-snowflake)
- Three-layer architecture: staging → intermediate → marts
- `stg_orders` — type casting, renaming, source metadata
- `int_orders_enriched` — total_amount, fulfillment_days, is_weekend
- `int_orders_quality_flags` — is_valid_quantity, is_valid_price, is_duplicate
- `fact_orders` — incremental model, unique_key=order_id
- `dim_product`, `dim_date`, `mart_data_quality`

---

## IAM Policy Summary

### Role: `streamcart-glue-role`
Attached to the AWS Glue job. Follows least-privilege principle.

**Managed Policies:**
- `AWSGlueServiceRole` — allows Glue to manage job execution

**Inline Policy: `streamcart-s3-access`**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadRaw",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::karina-ecommerce-lakehouse",
        "arn:aws:s3:::karina-ecommerce-lakehouse/ecommerce/raw/*"
      ]
    },
    {
      "Sid": "WriteCuratedAndQuarantine",
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject", "s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::karina-ecommerce-lakehouse/ecommerce/curated/*",
        "arn:aws:s3:::karina-ecommerce-lakehouse/ecommerce/quarantine/*"
      ]
    },
    {
      "Sid": "GlueScripts",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::karina-ecommerce-lakehouse/glue-scripts/*"
    }
  ]
}
```

**Inline Policy: `streamcart-cloudwatch-access`**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup", "logs:CreateLogStream",
        "logs:PutLogEvents", "logs:GetLogEvents",
        "logs:DescribeLogGroups", "logs:DescribeLogStreams"
      ],
      "Resource": "arn:aws:logs:eu-central-1:*:log-group:/aws-glue/*"
    }
  ]
}
```

### IAM User: `streamcart-pipeline-user`
Used by Airflow (`aws_default` connection) to trigger Glue and access S3.

**Inline Policy: `streamcart-glue-access`**
- `glue:GetJob`, `glue:StartJobRun`, `glue:GetJobRun`, `glue:GetJobRuns`, `glue:BatchStopJobRun`

**Inline Policy: `streamcart-s3-pipeline-access`**
- `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on `karina-ecommerce-lakehouse`

---

## Technology Decisions

| Decision | Choice | Reason |
|----------|--------|--------|
| CDC strategy | Watermark (updated_at) | Simple, no Debezium setup required |
| dbt adapter | dbt-snowflake | Familiar from previous homeworks |
| S3 → Snowflake | COPY INTO via External Stage | No additional tooling needed |
| COPY INTO strategy | TRUNCATE + reload | S3 curated is single source of truth |
| Glue DPUs | 2 (minimum) | Cost safety — dataset is 5-50k rows |
| Airflow executor | LocalExecutor | Single-node Docker setup |
