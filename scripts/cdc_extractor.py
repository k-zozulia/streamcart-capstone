from __future__ import annotations

"""
Watermark-based CDC extractor: reads changed rows from PostgreSQL
and lands them as JSON files in S3.

Supports two modes:
  FULL        — re-extracts ALL rows (initial load or recovery)
  INCREMENTAL — extracts only rows where updated_at > last watermark

Called by Airflow task extract_cdc via PythonOperator.
Can also be run standalone for testing.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal

import boto3
import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration — all values come from environment variables
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("SOURCE_DB_HOST", "localhost"),
    "port": int(os.getenv("SOURCE_DB_PORT", "5433")),
    "dbname": os.getenv("SOURCE_DB_NAME", "ecommerce"),
    "user": os.getenv("SOURCE_DB_USER", "ecommerce_user"),
    "password": os.getenv("SOURCE_DB_PASSWORD", "ecommerce_pass"),
}

S3_BUCKET = os.getenv("S3_BUCKET", "karina-ecommerce-lakehouse")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# S3 prefixes — match the layout defined in the assignment §4.3
RAW_PREFIX = "ecommerce/raw/orders"
FULL_PREFIX = "ecommerce/raw/orders/full_load"

# Fallback watermark if Airflow Variable is not set
DEFAULT_WATERMARK = "2020-01-01 00:00:00+00:00"
# ---------------------------------------------------------------------------
# S3 client factory — reads credentials from Airflow Connection if available,
# falls back to env vars / instance profile (for standalone use)
# ---------------------------------------------------------------------------


def get_s3_client():
    """
    Returns a boto3 S3 client.

    Priority:
      1. Airflow Connection 'aws_default' — used when running inside Airflow
      2. Env vars AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY — standalone use
      3. Instance profile / default credential chain — EC2/ECS environments

    This is why the Airflow Connection you created in the UI works here.
    """
    try:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="aws_default")
        session = hook.get_session()
        credentials = session.get_credentials().get_frozen_credentials()
        client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=credentials.access_key,
            aws_secret_access_key=credentials.secret_key,
            aws_session_token=credentials.token,
        )
        log.info("S3 client created using Airflow Connection 'aws_default'")
        return client
    except Exception as e:
        log.warning(f"Could not use Airflow Connection: {e} — falling back to env vars")
        return boto3.client("s3", region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# Helper: JSON serialiser that handles Decimal and datetime
# ---------------------------------------------------------------------------


def json_serial(obj):
    """
    Custom JSON serialiser for types that json.dumps() can't handle by default.

    PostgreSQL DECIMAL → Python Decimal → we convert to float
    PostgreSQL TIMESTAMP WITH TIME ZONE → Python datetime → ISO-8601 string
    """
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, datetime):
        # Always store as UTC ISO-8601 string: "2024-03-15T14:32:00+00:00"
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not JSON serialisable")


# ---------------------------------------------------------------------------
# Step 1 — Read watermark
# ---------------------------------------------------------------------------


def get_watermark_from_airflow() -> str:
    """
    Read last_cdc_watermark from Airflow Variables via the Airflow CLI
    (works when cdc_extractor.py is called from inside the Airflow container).

    Returns the watermark string or the DEFAULT_WATERMARK if not set.
    """
    try:
        # Import here so the script still works outside Airflow environment
        from airflow.models import Variable

        watermark = Variable.get("last_cdc_watermark", default_var=DEFAULT_WATERMARK)
        log.info(f"Watermark read from Airflow Variable: {watermark}")
        return watermark
    except ImportError:
        log.warning("Airflow not available — using DEFAULT_WATERMARK")
        return DEFAULT_WATERMARK
    except Exception as e:
        log.warning(f"Could not read Airflow Variable: {e} — using DEFAULT_WATERMARK")
        return DEFAULT_WATERMARK


def get_watermark(watermark_override: str = None) -> str:
    """
    Resolve the watermark to use for this run.

    Priority:
      1. CLI argument --watermark (for standalone testing)
      2. Airflow Variable last_cdc_watermark (when called from Airflow)
      3. DEFAULT_WATERMARK (fallback)
    """
    if watermark_override:
        log.info(f"Watermark override from CLI: {watermark_override}")
        return watermark_override
    return get_watermark_from_airflow()


# ---------------------------------------------------------------------------
# Step 2 — Extract rows from PostgreSQL
# ---------------------------------------------------------------------------


def extract_full_load(conn) -> list[dict]:
    """
    FULL mode: extract ALL rows from the orders table.
    Used for initial load or disaster recovery.
    """
    log.info("Running FULL LOAD — extracting all rows from orders table")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute("SELECT * FROM orders ORDER BY updated_at")
        rows = [dict(row) for row in cur.fetchall()]
    return rows


def extract_incremental(conn, watermark: str) -> list[dict]:
    """
    INCREMENTAL mode: extract only rows changed since the watermark.

    Query: SELECT * FROM orders WHERE updated_at > %s ORDER BY updated_at

    The ORDER BY updated_at ensures that if we process in batches and fail
    halfway, we can resume from the last successfully processed updated_at.

    The index idx_orders_updated_at (created in init.sql) makes this fast.
    """
    log.info(f"Running INCREMENTAL — extracting rows where updated_at > '{watermark}'")
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT *
            FROM   orders
            WHERE  updated_at > %s
            ORDER  BY updated_at
            """,
            (watermark,),
        )
        rows = [dict(row) for row in cur.fetchall()]
    return rows


# ---------------------------------------------------------------------------
# Step 3 — Build S3 destination path
# ---------------------------------------------------------------------------


def build_s3_key(mode: str, batch_ts: datetime) -> str:
    """
    Build the S3 object key based on mode and current timestamp.

    INCREMENTAL:
        ecommerce/raw/orders/date=2024-03-15/batch_20240315T143200Z.json

    FULL:
        ecommerce/raw/orders/full_load/full_20240315T143200Z.json

    The date= partition prefix lets AWS Glue and Athena automatically
    discover partitions without a crawler.
    """
    ts_str = batch_ts.strftime("%Y%m%dT%H%M%SZ")

    if mode == "full":
        return f"{FULL_PREFIX}/full_{ts_str}.json"
    else:
        date_str = batch_ts.strftime("%Y-%m-%d")
        return f"{RAW_PREFIX}/date={date_str}/batch_{ts_str}.json"


# ---------------------------------------------------------------------------
# Step 4 — Write to S3
# ---------------------------------------------------------------------------


def write_to_s3(rows: list[dict], s3_key: str) -> str:
    """
    Serialise rows as JSON and upload to S3.

    Format: one JSON object per line (newline-delimited JSON / NDJSON).
    This is the most Glue/Athena-friendly format — each line is one record.

    Returns the full S3 URI (s3://bucket/key).
    Raises an exception on failure — caller must NOT update watermark if this fails.
    """
    s3_client = get_s3_client()

    # Newline-delimited JSON: each row on its own line
    ndjson_content = "\n".join(json.dumps(row, default=json_serial) for row in rows)
    body = ndjson_content.encode("utf-8")

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=body,
        ContentType="application/x-ndjson",
    )

    s3_uri = f"s3://{S3_BUCKET}/{s3_key}"
    log.info(f"Written {len(rows)} rows to {s3_uri}  ({len(body):,} bytes)")
    return s3_uri


def delete_s3_prefix(prefix: str) -> int:
    """
    Delete all objects under a given S3 prefix.
    Used in FULL mode to clear the raw zone before re-landing all data.

    Returns count of deleted objects.
    """
    s3_client = get_s3_client()
    paginator = s3_client.get_paginator("list_objects_v2")

    deleted_count = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        objects = page.get("Contents", [])
        if not objects:
            continue
        delete_keys = [{"Key": obj["Key"]} for obj in objects]
        s3_client.delete_objects(
            Bucket=S3_BUCKET,
            Delete={"Objects": delete_keys},
        )
        deleted_count += len(delete_keys)

    log.info(f"Deleted {deleted_count} objects under s3://{S3_BUCKET}/{prefix}")
    return deleted_count


# ---------------------------------------------------------------------------
# Step 5 — Update watermark (ONLY after successful S3 write)
# ---------------------------------------------------------------------------


def update_watermark_in_airflow(new_watermark: str) -> None:
    """
    Update last_cdc_watermark Airflow Variable to the new value.

    CRITICAL: this is called ONLY after a successful S3 write.
    If S3 write fails, we raise before reaching this function,
    so the watermark stays at its old value → next run will retry.
    """
    try:
        from airflow.models import Variable

        Variable.set("last_cdc_watermark", new_watermark)
        log.info(f"Airflow Variable 'last_cdc_watermark' updated to: {new_watermark}")
    except ImportError:
        log.warning("Airflow not available — watermark NOT updated (standalone mode)")
    except Exception as e:
        # If we can't update the watermark after a successful S3 write,
        # log the error but don't fail — next run will re-extract some rows
        # (idempotent because Glue deduplicates by order_id)
        log.error(f"Could not update Airflow Variable: {e}")


# ---------------------------------------------------------------------------
# Step 6 — Log batch statistics
# ---------------------------------------------------------------------------


def log_batch_stats(rows: list[dict], s3_uri: str, mode: str) -> None:
    """
    Log key metrics for observability. These appear in Airflow task logs
    and are visible in the screenshot required by §5.3 item 4.
    """
    if not rows:
        log.info("Batch stats: 0 rows extracted — nothing to log")
        return

    updated_ats = [row["updated_at"] for row in rows if row.get("updated_at")]
    min_updated = min(updated_ats) if updated_ats else "N/A"
    max_updated = max(updated_ats) if updated_ats else "N/A"

    log.info("=" * 60)
    log.info("BATCH STATISTICS")
    log.info(f"  Mode            : {mode.upper()}")
    log.info(f"  Rows extracted  : {len(rows)}")
    log.info(f"  min(updated_at) : {min_updated}")
    log.info(f"  max(updated_at) : {max_updated}")
    log.info(f"  S3 destination  : {s3_uri}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# Main extraction function — called by Airflow PythonOperator
# ---------------------------------------------------------------------------


def run_extraction(mode: str, watermark_override: str = None) -> dict:
    """
    Full CDC extraction pipeline:
      1. Read watermark
      2. Connect to Postgres
      3. Extract rows (full or incremental)
      4. Write to S3
      5. Update watermark (only on success)
      6. Log stats

    Returns a result dict that Airflow can use via XCom:
        {
            "mode": "incremental",
            "rows_extracted": 127,
            "s3_uri": "s3://bucket/ecommerce/raw/orders/date=2024-03-15/batch_...json",
            "watermark_used": "2024-03-14 22:00:00+00:00",
            "new_watermark": "2024-03-15 14:32:00.123456+00:00",
        }
    """
    log.info("=" * 60)
    log.info(f"CDC EXTRACTOR STARTED  mode={mode.upper()}")
    log.info("=" * 60)

    batch_ts = datetime.now(timezone.utc)

    # ── Step 1: Watermark ────────────────────────────────────────────────
    watermark = get_watermark(watermark_override)

    # ── Step 2: Connect to Postgres ──────────────────────────────────────
    log.info(f"Connecting to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True  # read-only queries, no transaction needed
        log.info("PostgreSQL connection established ✓")
    except Exception as e:
        log.error(f"Failed to connect to PostgreSQL: {e}")
        raise

    try:
        # ── Step 3: Extract ──────────────────────────────────────────────
        if mode == "full":
            # FULL mode: clear the raw zone first, then re-extract everything
            log.info("FULL mode: clearing existing raw S3 prefix before re-landing")
            delete_s3_prefix(RAW_PREFIX + "/date=")  # only incremental partitions
            delete_s3_prefix(FULL_PREFIX)
            rows = extract_full_load(conn)
        else:
            rows = extract_incremental(conn, watermark)

        log.info(f"Extracted {len(rows)} rows from PostgreSQL")

        # ── Step 4: Write to S3 ──────────────────────────────────────────
        if len(rows) == 0:
            log.info("No rows to extract — skipping S3 write and watermark update")
            return {
                "mode": mode,
                "rows_extracted": 0,
                "s3_uri": None,
                "watermark_used": watermark,
                "new_watermark": watermark,  # unchanged
            }

        s3_key = build_s3_key(mode, batch_ts)

        # This will raise if S3 write fails — watermark is NOT updated below
        s3_uri = write_to_s3(rows, s3_key)

        # ── Step 5: Update watermark (ONLY after successful S3 write) ────
        # New watermark = max(updated_at) in this batch
        # Next run will start from this point
        new_watermark = max(
            row["updated_at"] for row in rows if row.get("updated_at")
        ).isoformat()

        update_watermark_in_airflow(new_watermark)

        # ── Step 6: Log stats ────────────────────────────────────────────
        log_batch_stats(rows, s3_uri, mode)

        result = {
            "mode": mode,
            "rows_extracted": len(rows),
            "s3_uri": s3_uri,
            "s3_key": s3_key,
            "watermark_used": watermark,
            "new_watermark": new_watermark,
        }

        log.info("CDC EXTRACTOR COMPLETED ✓")
        return result

    finally:
        conn.close()
        log.info("PostgreSQL connection closed")


# ---------------------------------------------------------------------------
# Airflow entry point
# Called by PythonOperator in ecommerce_pipeline.py like:
#   from scripts.cdc_extractor import run_extraction
#   PythonOperator(task_id='extract_cdc', python_callable=run_extraction, op_kwargs={'mode': '{{ var.value.pipeline_mode }}'})
# ---------------------------------------------------------------------------


def airflow_extract(**context) -> dict:
    """
    Wrapper for Airflow PythonOperator.
    Reads mode from Airflow Variable and pushes result to XCom.
    """
    try:
        from airflow.models import Variable

        mode = Variable.get("pipeline_mode", default_var="incremental").lower()
    except Exception:
        mode = "incremental"

    log.info(f"Airflow context: pipeline_mode = {mode}")
    result = run_extraction(mode=mode)

    # Push result to XCom so validate_extract task can read it
    context["ti"].xcom_push(key="cdc_result", value=result)
    return result


# ---------------------------------------------------------------------------
# CLI entry point — for standalone testing without Airflow
# ---------------------------------------------------------------------------


def parse_args():
    parser = argparse.ArgumentParser(
        description="StreamCart CDC Extractor — reads changes from Postgres and lands to S3"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Extraction mode: 'full' re-extracts all rows, 'incremental' uses watermark (default: incremental)",
    )
    parser.add_argument(
        "--watermark",
        type=str,
        default=None,
        help="Override watermark timestamp, e.g. '2024-01-01 00:00:00' (incremental mode only)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        result = run_extraction(mode=args.mode, watermark_override=args.watermark)
        log.info(f"Result: {json.dumps(result, default=json_serial, indent=2)}")
        sys.exit(0)
    except Exception as e:
        log.error(f"Extraction failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
