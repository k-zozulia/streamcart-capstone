# StreamCart Analytics Platform

End-to-end production data pipeline for e-commerce analytics.
**Stack:** PostgreSQL → Python CDC → AWS S3 → AWS Glue → Snowflake → dbt → Power BI

---

## Architecture
```
PostgreSQL (Docker) → CDC Extractor → S3 Raw → Glue ETL → S3 Curated
                                                               ↓
                                                    COPY INTO Snowflake
                                                               ↓
                                                        dbt Models
                                                               ↓
                                                          Power BI
```

Full architecture details: [docs/architecture.md](docs/architecture.md)

---

## Project Structure
```
streamcart-capstone/
├── docker/
│   ├── docker-compose.yml       # PostgreSQL + Airflow services
│   └── init.sql                 # DDL + seed data (5000 rows)
├── scripts/
│   ├── cdc_extractor.py         # Watermark-based CDC extraction
│   └── seed_updates.py          # Simulates ongoing inserts/updates
├── airflow/
│   └── dags/
│       ├── ecommerce_pipeline.py  # Main DAG (7 tasks, daily schedule)
│       └── ecommerce_full_load.py # Manual full load DAG (trigger only)
├── glue_jobs/
│   └── raw_to_curated.py        # PySpark ETL job
├── dbt/
│   ├── dbt_project.yml
│   ├── packages.yml
│   └── models/
│       ├── staging/             # stg_orders
│       ├── intermediate/        # int_orders_enriched, int_orders_quality_flags
│       └── marts/               # fact_orders, dim_product, dim_date, mart_data_quality
├── sql/
│   └── analytics_queries.sql    # 7 analytical queries
├── docs/
│   ├── architecture.md          # Architecture diagram + IAM policies
│   ├── cdc_strategy.md          # CDC approach + late arrival handling
│   └── teardown_checklist.md    # AWS cleanup steps
└── README.md
```

---

## Prerequisites

- Docker Desktop
- Python 3.8+
- AWS CLI configured (`aws configure`)
- Snowflake account
- dbt-snowflake (`pip install dbt-snowflake==1.8.4`)

---

## Setup Guide

### Step 1 — AWS Setup

1. Set a billing alert for $5 USD in AWS Console → Billing → Budgets
2. Create S3 bucket:
```bash
aws s3 mb s3://karina-ecommerce-lakehouse --region eu-central-1
```

3. Create IAM user `streamcart-pipeline-user` with policies in `docs/architecture.md`
4. Create IAM role `streamcart-glue-role` with policies in `docs/architecture.md`
5. Upload Glue script:
```bash
aws s3 cp glue_jobs/raw_to_curated.py \
  s3://karina-ecommerce-lakehouse/glue-scripts/raw_to_curated.py
```

6. Create Glue job named `streamcart-raw-to-curated` pointing to the uploaded script, using `streamcart-glue-role`, 2 DPUs

### Step 2 — Snowflake Setup

Run in Snowflake Worksheet as ACCOUNTADMIN:
```sql
-- Storage Integration
CREATE STORAGE INTEGRATION streamcart_s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/streamcart-glue-role'
  STORAGE_ALLOWED_LOCATIONS = (
    's3://karina-ecommerce-lakehouse/ecommerce/curated/',
    's3://karina-ecommerce-lakehouse/ecommerce/quarantine/'
  );

DESC INTEGRATION streamcart_s3_integration;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- Update trust policy on streamcart-glue-role in AWS IAM
```
```sql
-- Database and schemas
USE ROLE SYSADMIN;
CREATE DATABASE IF NOT EXISTS STREAMCART_DB;
CREATE SCHEMA IF NOT EXISTS STREAMCART_DB.RAW;

-- File format
CREATE OR REPLACE FILE FORMAT STREAMCART_DB.RAW.parquet_format
  TYPE = 'PARQUET' SNAPPY_COMPRESSION = TRUE;

-- Stages
CREATE OR REPLACE STAGE STREAMCART_DB.RAW.streamcart_curated_stage
  STORAGE_INTEGRATION = streamcart_s3_integration
  URL = 's3://karina-ecommerce-lakehouse/ecommerce/curated/orders/'
  FILE_FORMAT = parquet_format;

CREATE OR REPLACE STAGE STREAMCART_DB.RAW.streamcart_quarantine_stage
  STORAGE_INTEGRATION = streamcart_s3_integration
  URL = 's3://karina-ecommerce-lakehouse/ecommerce/quarantine/orders/'
  FILE_FORMAT = parquet_format;

-- Tables
CREATE TABLE IF NOT EXISTS STREAMCART_DB.RAW.orders_curated (
  order_id INTEGER, customer_id INTEGER, product_id INTEGER,
  quantity INTEGER, unit_price FLOAT, status VARCHAR,
  created_at TIMESTAMP_TZ, updated_at TIMESTAMP_TZ,
  order_date DATE, record_type VARCHAR, pipeline_run_id VARCHAR
);

CREATE TABLE IF NOT EXISTS STREAMCART_DB.RAW.orders_quarantine (
  order_id INTEGER, customer_id INTEGER, product_id INTEGER,
  quantity INTEGER, unit_price FLOAT, status VARCHAR,
  created_at TIMESTAMP_TZ, updated_at TIMESTAMP_TZ,
  order_date DATE, rejection_reason VARCHAR
);
```

### Step 3 — dbt Setup
```bash
cd dbt
pip install dbt-snowflake==1.8.4
dbt deps
dbt debug  # verify Snowflake connection
```

`~/.dbt/profiles.yml`:
```yaml
streamcart:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: jfrnpct-fk56390
      user: karinazozulia23
      password: <your_password>
      role: SYSADMIN
      database: STREAMCART_DB
      warehouse: COMPUTE_WH
      schema: STREAMCART
      threads: 4
```

### Step 4 — Start Docker
```bash
# Create .env file
cat > docker/.env << 'ENV'
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=eu-central-1
S3_BUCKET=karina-ecommerce-lakehouse
ENV

cd docker
docker-compose up -d

# Wait ~2 minutes, then verify
docker exec --user airflow streamcart-airflow-webserver dbt --version
```

### Step 5 — Configure Airflow

Open http://localhost:8080 (admin/admin)

**Admin → Connections → +:**

| Connection Id | Type | Details |
|--------------|------|---------|
| `aws_default` | Amazon Web Services | Login=ACCESS_KEY, Password=SECRET_KEY, Extra=`{"region_name":"eu-central-1"}` |
| `postgres_source` | Postgres | Host=source-postgres, DB=ecommerce, Login=ecommerce_user, Pass=ecommerce_pass, Port=5432 |
| `slack_default` | Slack Incoming Webhook | Slack Webhook Endpoint=your_webhook_url, Webhook Token=T.../B.../... |

**Admin → Variables → +:**

| Key | Value |
|-----|-------|
| `pipeline_mode` | `full` |
| `last_cdc_watermark` | `2020-01-01 00:00:00` |
| `s3_bucket` | `karina-ecommerce-lakehouse` |
| `glue_job_name` | `streamcart-raw-to-curated` |
| `snowflake_account` | `jfrnpct-fk56390` |
| `snowflake_user` | `karinazozulia23` |
| `snowflake_password` | `<your_password>` |

### Step 6 — Seed Data
```bash
python scripts/seed_updates.py
```

### Step 7 — Run Pipeline

**Scheduled incremental pipeline (runs daily automatically):**

In Airflow UI → `ecommerce_pipeline` → **Trigger DAG ▶️**

Expected task sequence (all green):
1. `check_source_availability` (~2s)
2. `extract_cdc` (~10s)
3. `validate_extract` (~1s)
4. `trigger_glue_job` (~3-5 min)
5. `copy_into_snowflake` (~30s)
6. `run_dbt_models` (~2 min)
7. `refresh_data_quality_summary` (~2s)

**Initial full load or disaster recovery:**

In Airflow UI → `ecommerce_full_load` → **Trigger DAG ▶️**

This DAG automatically sets `pipeline_mode=full`, resets the watermark to `2020-01-01`,
runs the complete pipeline, then resets back to `incremental` mode.

---

## Running Incremental Updates
```bash
# Add new data to PostgreSQL
python scripts/seed_updates.py

# Pipeline mode is already 'incremental' after first full run
# Just trigger the DAG again
```

---

## dbt Commands
```bash
cd dbt

# Run all models
dbt run --profiles-dir ~/.dbt --project-dir .

# Run only incremental models
dbt run --select tag:incremental --profiles-dir ~/.dbt --project-dir .

# Run tests
dbt test --profiles-dir ~/.dbt --project-dir .

# Generate and serve docs
dbt docs generate --profiles-dir ~/.dbt --project-dir .
dbt docs serve
```

---

## Short-Circuit Demo (0 rows extracted)

To demonstrate the ShortCircuitOperator:

1. Set `last_cdc_watermark` = `2030-01-01 00:00:00` in Airflow Variables
2. Trigger DAG
3. `validate_extract` will short-circuit — tasks 4-7 will be skipped (grey)
4. Reset watermark back to run normally

---

## Failure Alerts

Pipeline failures trigger a Slack notification to the configured webhook.
To set up: create a Slack Incoming Webhook and add it as `slack_default` connection in Airflow
(Admin → Connections → slack_default → Slack Incoming Webhook).

---

## Snowflake Note

This project uses `dbt-snowflake` with data loaded via `COPY INTO` from S3 External Stage,
rather than `dbt-glue`. This approach was chosen because:
- Snowflake was already configured from previous homeworks
- COPY INTO provides reliable, idempotent loading
- No additional Glue catalog configuration required

See `docs/architecture.md` for full details.
