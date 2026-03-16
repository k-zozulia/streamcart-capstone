"""
Manual full load DAG for StreamCart Analytics Platform.
Triggers a complete re-extraction of all orders from PostgreSQL.
Use for: initial load, disaster recovery, schema changes.

Trigger manually only — no schedule.
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


DEFAULT_ARGS = {
    "owner": "streamcart",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def set_full_mode_callable(**context) -> None:
    Variable.set("pipeline_mode", "full")
    Variable.set("last_cdc_watermark", "2020-01-01 00:00:00")
    log.info("pipeline_mode set to 'full', watermark reset to 2020-01-01")


def extract_full_callable(**context) -> dict:
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from cdc_extractor import run_extraction

    result = run_extraction(mode="full")
    context["ti"].xcom_push(key="cdc_result", value=result)
    log.info(f"Full extraction result: {json.dumps(result, default=str)}")

    Variable.set("pipeline_mode", "incremental")
    log.info("pipeline_mode reset to 'incremental' after full load")

    return result


def validate_extract_callable(**context) -> bool:
    ti = context["ti"]
    cdc_result = ti.xcom_pull(task_ids="extract_full", key="cdc_result")

    if not cdc_result:
        log.warning("No XCom result — skipping downstream")
        return False

    rows_extracted = cdc_result.get("rows_extracted", 0)
    s3_uri = cdc_result.get("s3_uri")

    log.info(f"rows_extracted={rows_extracted}  s3_uri={s3_uri}")

    if rows_extracted == 0 or s3_uri is None:
        log.info("0 rows extracted — short-circuiting")
        return False

    return True

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

def refresh_data_quality_callable(**context) -> None:
    import os
    import psycopg2

    ti           = context["ti"]
    run_id       = context["run_id"]
    logical_date = context["logical_date"].date()

    cdc_result     = ti.xcom_pull(task_ids="extract_full", key="cdc_result") or {}
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


with DAG(
    dag_id="ecommerce_full_load",
    default_args=DEFAULT_ARGS,
    description="Manual full load — re-extracts all orders from scratch",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["streamcart", "full-load"],
) as dag:

    set_full_mode = PythonOperator(
        task_id="set_full_mode",
        python_callable=set_full_mode_callable,
    )

    extract_full = PythonOperator(
        task_id="extract_full",
        python_callable=extract_full_callable,
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
                "{{ ti.xcom_pull(task_ids='extract_full', key='cdc_result')"
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

            echo "=== dbt run (all models — full load) ==="
            dbt run \
                --profiles-dir $DBT_PROFILES_DIR \
                --project-dir $DBT_PROJECT_DIR

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
        set_full_mode
        >> extract_full
        >> validate_extract
        >> trigger_glue_job
        >> copy_into_snowflake
        >> run_dbt_models
        >> refresh_data_quality_summary
    )