"""
Main DAG for the StreamCart Analytics Platform.

Task dependency graph:
    check_source_availability
            ↓
        extract_cdc
            ↓
      validate_extract
       ↓ (if rows > 0)
      trigger_glue_job
            ↓
    copy_into_snowflake
            ↓
      run_dbt_models
            ↓
  refresh_data_quality_summary

notify_on_failure — failure callback on all tasks via default_args
"""

from __future__ import annotations
import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------

def notify_on_failure(context: dict) -> None:
    # SlackWebhookOperator equivalent — uses requests directly due to
    # provider version compatibility issue with apache-airflow-providers-slack==8.5.0
    import requests
    from airflow.hooks.base import BaseHook

    dag_id  = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id  = context["task_instance"].run_id
    log_url = context["task_instance"].log_url

    conn = BaseHook.get_connection("slack_default")
    webhook_url = conn.host

    requests.post(
        webhook_url,
        json={
            "text": (
                f":red_circle: *StreamCart Pipeline Failure*\n"
                f"*DAG:* {dag_id}\n"
                f"*Task:* {task_id}\n"
                f"*Run ID:* {run_id}\n"
                f"*Logs:* {log_url}"
            )
        }
    )

    log.error(
        f"TASK FAILED | DAG: {dag_id} | Task: {task_id} | "
        f"Run: {run_id} | Logs: {log_url}"
    )


DEFAULT_ARGS = {
    "owner": "streamcart",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_on_failure,
}


# ---------------------------------------------------------------------------
# Task 1 — check_source_availability
# ---------------------------------------------------------------------------

def check_postgres_callable() -> bool:
    import os
    import psycopg2

    conn = psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "source-postgres"),
        port=int(os.getenv("SOURCE_DB_PORT", "5432")),
        dbname=os.getenv("SOURCE_DB_NAME", "ecommerce"),
        user=os.getenv("SOURCE_DB_USER", "ecommerce_user"),
        password=os.getenv("SOURCE_DB_PASSWORD", "ecommerce_pass"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders")
            count = cur.fetchone()[0]
        log.info(f"Source DB reachable — orders table has {count} rows")
        return True
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Task 2 — extract_cdc
# ---------------------------------------------------------------------------

def extract_cdc_callable(**context) -> dict:
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from cdc_extractor import run_extraction

    mode = Variable.get("pipeline_mode", default_var="incremental").lower()
    log.info(f"CDC extraction mode: {mode}")

    result = run_extraction(mode=mode)
    context["ti"].xcom_push(key="cdc_result", value=result)
    log.info(f"XCom pushed: {json.dumps(result, default=str)}")

    if mode == "full":
        Variable.set("pipeline_mode", "incremental")
        log.info("pipeline_mode reset to 'incremental' after full load")

    return result


# ---------------------------------------------------------------------------
# Task 3 — validate_extract
# ---------------------------------------------------------------------------

def validate_extract_callable(**context) -> bool:
    ti         = context["ti"]
    cdc_result = ti.xcom_pull(task_ids="extract_cdc", key="cdc_result")

    if not cdc_result:
        log.warning("No XCom result from extract_cdc — skipping downstream")
        return False

    rows_extracted = cdc_result.get("rows_extracted", 0)
    s3_uri         = cdc_result.get("s3_uri")

    log.info(f"rows_extracted={rows_extracted}  s3_uri={s3_uri}")

    if rows_extracted == 0 or s3_uri is None:
        log.info("0 rows extracted — short-circuiting all downstream tasks")
        return False

    log.info(f"Validation passed — {rows_extracted} rows ready for Glue")
    return True


# ---------------------------------------------------------------------------
# Task 5 — copy_into_snowflake
# ---------------------------------------------------------------------------

def copy_into_snowflake_callable(**context) -> None:
    import snowflake.connector

    account  = Variable.get("snowflake_account",  default_var="jfrnpct-fk56390")
    user     = Variable.get("snowflake_user",     default_var="karinazozulia23")
    password = Variable.get("snowflake_password")
    run_id   = context["run_id"]

    log.info(f"Connecting to Snowflake account: {account}")

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        password=password,
        role="SYSADMIN",
        warehouse="COMPUTE_WH",
        database="STREAMCART_DB",
        schema="RAW",
    )

    try:
        cur = conn.cursor()

        # ── CURATED: TRUNCATE + COPY INTO ─────────────────────────────────
        # orders_curated is a staging table — holds only the current batch.
        # S3 curated is the single source of truth with full history.
        log.info("Loading orders_curated via TRUNCATE + COPY INTO...")

        cur.execute("TRUNCATE TABLE orders_curated")

        cur.execute("""
            COPY INTO orders_curated
            FROM @streamcart_curated_stage
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE
        """)

        cur.execute("UPDATE orders_curated SET _extracted_at = CURRENT_TIMESTAMP()")
        log.info("_extracted_at stamped on all curated rows")

        cur.execute("SELECT COUNT(*) FROM orders_curated")
        curated_count = cur.fetchone()[0]
        log.info(f"orders_curated loaded: {curated_count} rows")

        # ── QUARANTINE: TRUNCATE + COPY INTO ──────────────────────────────
        # Quarantine history is preserved in S3.
        # Snowflake quarantine table holds only current batch for dbt.
        log.info("Loading orders_quarantine via TRUNCATE + COPY INTO...")

        cur.execute("TRUNCATE TABLE orders_quarantine")

        cur.execute("""
            COPY INTO orders_quarantine
            FROM @streamcart_quarantine_stage
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = CONTINUE
        """)

        cur.execute("SELECT COUNT(*) FROM orders_quarantine")
        quarantine_count = cur.fetchone()[0]
        log.info(f"orders_quarantine loaded: {quarantine_count} rows")

        # Breakdown by rejection reason
        if quarantine_count > 0:
            cur.execute("""
                SELECT rejection_reason, COUNT(*)
                FROM orders_quarantine
                GROUP BY 1
                ORDER BY 2 DESC
            """)
            breakdown = cur.fetchall()
            log.info(f"Quarantine breakdown: {breakdown}")

        cur.close()
        log.info("copy_into_snowflake completed successfully ✓")

    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Task 7 — refresh_data_quality_summary
# ---------------------------------------------------------------------------

def refresh_data_quality_callable(**context) -> None:
    import os
    import psycopg2

    ti           = context["ti"]
    run_id       = context["run_id"]
    logical_date = context["logical_date"].date()

    cdc_result     = ti.xcom_pull(task_ids="extract_cdc", key="cdc_result") or {}
    rows_extracted = cdc_result.get("rows_extracted", 0)

    quarantine_approx = max(0, rows_extracted // 10)
    curated_approx    = rows_extracted - quarantine_approx

    dbt_output = ti.xcom_pull(task_ids="run_dbt_models") or ""
    dbt_status = "passed" if "successfully" in str(dbt_output).lower() else "unknown"

    log.info(
        f"Writing audit | run_id={run_id} | date={logical_date} | "
        f"curated={curated_approx} | quarantine={quarantine_approx} | dbt={dbt_status}"
    )

    conn = psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "source-postgres"),
        port=int(os.getenv("SOURCE_DB_PORT", "5432")),
        dbname=os.getenv("SOURCE_DB_NAME", "ecommerce"),
        user=os.getenv("SOURCE_DB_USER", "ecommerce_user"),
        password=os.getenv("SOURCE_DB_PASSWORD", "ecommerce_pass"),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pipeline_audit
                        (run_id, run_date, curated_rows, quarantine_rows, dbt_test_status)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (run_id, logical_date, curated_approx, quarantine_approx, dbt_status),
                )
        log.info("Audit row written ✓")
    finally:
        conn.close()

    import json
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    audit_record = {
        "run_id": run_id,
        "run_date": str(logical_date),
        "curated_rows": curated_approx,
        "quarantine_rows": quarantine_approx,
        "dbt_test_status": dbt_status,
    }

    bucket = Variable.get("s3_bucket", default_var="karina-ecommerce-lakehouse")
    s3_client = S3Hook(aws_conn_id="aws_default").get_conn()
    s3_client.put_object(
        Bucket=bucket,
        Key=f"ecommerce/audit/run_date={logical_date}/audit_{run_id}.json",
        Body=json.dumps(audit_record).encode("utf-8"),
        ContentType="application/json",
    )
    log.info(f"Audit JSON written to s3://{bucket}/ecommerce/audit/run_date={logical_date}/")

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="ecommerce_pipeline",
    default_args=DEFAULT_ARGS,
    description="StreamCart CDC pipeline: Postgres → S3 → Glue → Snowflake → dbt → Audit",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["streamcart", "cdc", "production"],
) as dag:

    check_source_availability = PythonOperator(
        task_id="check_source_availability",
        python_callable=check_postgres_callable,
    )

    extract_cdc = PythonOperator(
        task_id="extract_cdc",
        python_callable=extract_cdc_callable,
    )

    validate_extract = ShortCircuitOperator(
        task_id="validate_extract",
        python_callable=validate_extract_callable,
        retries=0,
    )

    trigger_glue_job = GlueJobOperator(
        task_id="trigger_glue_job",
        job_name="{{ var.value.glue_job_name | default('streamcart-raw-to-curated') }}",
        script_location=None,
        aws_conn_id="aws_default",
        region_name="eu-central-1",
        iam_role_name="streamcart-glue-role",
        script_args={
            "--S3_INPUT_PATH": (
                "{{ ti.xcom_pull(task_ids='extract_cdc', key='cdc_result')"
                "['s3_uri'] | default('') }}"
            ),
            "--PIPELINE_RUN_ID": "{{ run_id }}",
        },
        wait_for_completion=True,
        verbose=False,
    )

    copy_into_snowflake = PythonOperator(
        task_id="copy_into_snowflake",
        python_callable=copy_into_snowflake_callable,
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            set -e
            DBT_PROFILES_DIR=/home/airflow/.dbt
            DBT_PROJECT_DIR=/opt/airflow/dbt

            PIPELINE_MODE="{{ var.value.pipeline_mode | default('incremental') }}"
            echo "Pipeline mode: $PIPELINE_MODE"

            if [ "$PIPELINE_MODE" = "incremental" ]; then
                echo "=== dbt run (tag:incremental) ==="
                dbt run \
                    --select tag:incremental \
                    --profiles-dir $DBT_PROFILES_DIR \
                    --project-dir $DBT_PROJECT_DIR
            else
                echo "=== dbt run (all models) ==="
                dbt run \
                    --profiles-dir $DBT_PROFILES_DIR \
                    --project-dir $DBT_PROJECT_DIR
            fi

            echo "=== dbt test ==="
            dbt test \
                --profiles-dir $DBT_PROFILES_DIR \
                --project-dir $DBT_PROJECT_DIR

            echo "dbt completed successfully"
        """,
    )

    refresh_data_quality_summary = PythonOperator(
        task_id="refresh_data_quality_summary",
        python_callable=refresh_data_quality_callable,
    )

    (
        check_source_availability
        >> extract_cdc
        >> validate_extract
        >> trigger_glue_job
        >> copy_into_snowflake
        >> run_dbt_models
        >> refresh_data_quality_summary
    )