"""
Extended Glue ETL job for StreamCart capstone.
Reads CDC extracts from S3 raw zone, transforms to curated Parquet.
"""

import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, lower, trim, to_date, hour, when, lit,
    current_timestamp
)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, DateType
)
from datetime import datetime

# ---------------------------------------------------------------------------
# Job parameters
# ---------------------------------------------------------------------------
# S3_INPUT_PATH — passed by Airflow trigger_glue_job task
# PIPELINE_RUN_ID — Airflow run_id, used for audit trail
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'S3_INPUT_PATH', 'PIPELINE_RUN_ID']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BUCKET         = "karina-ecommerce-lakehouse"
S3_INPUT_PATH  = args['S3_INPUT_PATH']     # dynamic — passed from Airflow
PIPELINE_RUN_ID = args['PIPELINE_RUN_ID']  # Airflow run_id for audit
CURATED_PATH   = f"s3://{BUCKET}/ecommerce/curated/orders/"
QUARANTINE_PATH = f"s3://{BUCKET}/ecommerce/quarantine/orders/"

print(f"Job started at: {datetime.now().isoformat()}")
print(f"S3_INPUT_PATH:  {S3_INPUT_PATH}")
print(f"PIPELINE_RUN_ID: {PIPELINE_RUN_ID}")

# ---------------------------------------------------------------------------
# Schema — matches init.sql orders table
# unit_price (not price — updated from HW4)
# ---------------------------------------------------------------------------
raw_schema = StructType([
    StructField("order_id",    StringType(),    True),
    StructField("customer_id", StringType(),    True),
    StructField("product_id",  StringType(),    True),
    StructField("quantity",    IntegerType(),   True),
    StructField("unit_price",  DoubleType(),    True),   # renamed from HW4
    StructField("status",      StringType(),    True),
    StructField("created_at",  TimestampType(), True),
    StructField("updated_at",  TimestampType(), True),
])

# ---------------------------------------------------------------------------
# Step 1 — Read raw CDC extract
# S3_INPUT_PATH points to a specific batch file or date partition
# ---------------------------------------------------------------------------
print(f"Reading raw data from: {S3_INPUT_PATH}")

raw_df = spark.read \
    .schema(raw_schema) \
    .json(S3_INPUT_PATH)   # NDJSON format written by cdc_extractor.py

input_count = raw_df.count()
print(f"INPUT rows: {input_count}")

# ---------------------------------------------------------------------------
# Step 2 — Filter missing order_id → quarantine
# ---------------------------------------------------------------------------
valid_df   = raw_df.filter(col("order_id").isNotNull())
missing_df = raw_df.filter(col("order_id").isNull())

missing_count = missing_df.count()
print(f"Missing order_id: {missing_count}")

# ---------------------------------------------------------------------------
# Step 3 — Filter invalid quantity / unit_price → quarantine
# ---------------------------------------------------------------------------
valid_values_df   = valid_df.filter(
    (col("quantity") > 0) & (col("unit_price") >= 0)
)
invalid_values_df = valid_df.filter(
    (col("quantity") <= 0) | (col("unit_price") < 0)
)

invalid_values_count = invalid_values_df.count()
print(f"Invalid quantity/price: {invalid_values_count}")

# ---------------------------------------------------------------------------
# Step 4 — Normalize status values
# ---------------------------------------------------------------------------
VALID_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled"]

def normalize_status(df):
    df = df.withColumn("status", lower(trim(col("status"))))
    df = df.withColumn(
        "status",
        when(col("status").isin(["complete", "done"]), "completed")
        .when(col("status") == "canceled", "cancelled")
        .otherwise(col("status"))
    )
    return df

valid_values_df = normalize_status(valid_values_df)

# Rows with invalid status after normalization → quarantine
valid_status_df   = valid_values_df.filter(col("status").isin(VALID_STATUSES))
invalid_status_df = valid_values_df.filter(~col("status").isin(VALID_STATUSES))

invalid_status_count = invalid_status_df.count()
print(f"Invalid status: {invalid_status_count}")

# ---------------------------------------------------------------------------
# Step 5 — Deduplicate (keep latest updated_at per order_id)
# ---------------------------------------------------------------------------
from pyspark.sql.window import Window

window_spec = Window.partitionBy("order_id").orderBy(col("updated_at").desc())
deduped_df = valid_status_df \
    .withColumn("_rank", F.rank().over(window_spec)) \
    .filter(col("_rank") == 1) \
    .drop("_rank")

deduped_count = deduped_df.count()
print(f"After dedup: {deduped_count}")

# ---------------------------------------------------------------------------
# Step 6 — Derive columns
# ---------------------------------------------------------------------------
deduped_df = deduped_df \
    .withColumn("order_date",    to_date(col("updated_at"))) \
    .withColumn("hour_of_day",   hour(col("updated_at"))) \
    .withColumn("total_amount",  col("quantity") * col("unit_price")) \
    .withColumn("is_weekend",    (F.dayofweek(col("updated_at")).isin([1, 7])).cast("boolean"))

# ---------------------------------------------------------------------------
# Step 7 — Detect INSERT vs UPDATE
# Compare incoming order_ids with existing curated partition
#
# Logic:
#   - Read existing order_ids from curated Parquet
#   - If order_id already exists in curated → record_type = UPDATE
#   - If order_id is new → record_type = INSERT
# ---------------------------------------------------------------------------
try:
    existing_df = spark.read.parquet(CURATED_PATH)
    existing_ids = existing_df.select("order_id").distinct()

    # Left join: match incoming rows with existing curated order_ids
    deduped_with_type = deduped_df.join(
        existing_ids.withColumnRenamed("order_id", "existing_order_id"),
        deduped_df["order_id"] == col("existing_order_id"),
        how="left"
    ).withColumn(
        "record_type",
        when(col("existing_order_id").isNotNull(), lit("UPDATE"))
        .otherwise(lit("INSERT"))
    ).drop("existing_order_id")

    print("Curated partition found — detecting INSERT vs UPDATE")

except Exception as e:
    # First run — no curated data exists yet, all rows are INSERTs
    print(f"No existing curated data (first run or empty): {e}")
    deduped_with_type = deduped_df.withColumn("record_type", lit("INSERT"))

# Add pipeline_run_id for audit trail
deduped_with_type = deduped_with_type \
    .withColumn("pipeline_run_id", lit(PIPELINE_RUN_ID))

# Count INSERTs and UPDATEs for CloudWatch logging
insert_count = deduped_with_type.filter(col("record_type") == "INSERT").count()
update_count = deduped_with_type.filter(col("record_type") == "UPDATE").count()
print(f"INSERT rows: {insert_count}")
print(f"UPDATE rows: {update_count}")

# ---------------------------------------------------------------------------
# Step 8 — Build quarantine dataframe
# Combine all rejected rows with rejection_reason column
# ---------------------------------------------------------------------------
quarantine_parts = []

if missing_count > 0:
    q = missing_df.withColumn("rejection_reason", lit("missing_order_id"))
    q = q.withColumn("order_date",
        when(col("updated_at").isNotNull(), to_date(col("updated_at")))
        .otherwise(lit(None).cast(DateType()))
    )
    quarantine_parts.append(q)

if invalid_values_count > 0:
    q = invalid_values_df.withColumn("rejection_reason", lit("invalid_quantity_or_price"))
    q = q.withColumn("order_date",
        when(col("updated_at").isNotNull(), to_date(col("updated_at")))
        .otherwise(lit(None).cast(DateType()))
    )
    quarantine_parts.append(q)

if invalid_status_count > 0:
    q = invalid_status_df.withColumn("rejection_reason", lit("invalid_status"))
    q = q.withColumn("order_date",
        when(col("updated_at").isNotNull(), to_date(col("updated_at")))
        .otherwise(lit(None).cast(DateType()))
    )
    quarantine_parts.append(q)

quarantine_count = 0
if quarantine_parts:
    quarantine_df = quarantine_parts[0]
    for df in quarantine_parts[1:]:
        quarantine_df = quarantine_df.unionByName(df, allowMissingColumns=True)

    quarantine_count = quarantine_df.count()
    print(f"QUARANTINE rows: {quarantine_count}")

    # Log breakdown by rejection reason
    quarantine_stats = quarantine_df.groupBy("rejection_reason").count().collect()
    print("Quarantine breakdown:")
    for row in quarantine_stats:
        print(f"  {row['rejection_reason']}: {row['count']}")

    quarantine_df.write.mode("append").parquet(QUARANTINE_PATH)
else:
    print("QUARANTINE rows: 0")

# ---------------------------------------------------------------------------
# Step 9 — Write curated Parquet (partitioned by order_date)
# mode=append so each pipeline run adds to the partition
# Glue MERGE handles updates via record_type column
# ---------------------------------------------------------------------------
curated_count = deduped_with_type.count()
print(f"CURATED rows: {curated_count}")

deduped_with_type.write \
    .mode("append") \
    .partitionBy("order_date") \
    .parquet(CURATED_PATH)

# ---------------------------------------------------------------------------
# Step 10 — CloudWatch summary log
# Structured format so it's easy to query in CloudWatch Insights:
#   fields @message | filter @message like "PIPELINE_SUMMARY"
# ---------------------------------------------------------------------------
print(
    f"PIPELINE_SUMMARY | "
    f"run_id={PIPELINE_RUN_ID} | "
    f"input={input_count} | "
    f"insert={insert_count} | "
    f"update={update_count} | "
    f"quarantine={quarantine_count} | "
    f"curated={curated_count}"
)

print(f"Job ended at: {datetime.now().isoformat()}")
job.commit()