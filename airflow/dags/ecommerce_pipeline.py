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


def notify_on_failure(context: dict) -> None:
    """
    Failure callback attached to all tasks via default_args.
    Logs failure details. Replace log.error with Slack/Email in production.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    run_id = context["task_instance"].run_id
    log_url = context["task_instance"].log_url

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


def check_postgres_callable() -> bool:
    """
    Verifies PostgreSQL source DB is reachable and orders table exists.
    Raises on failure → Airflow marks task failed and retries (up to 2x).
    """
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
        log.info(f"Source DB reachable -  orders table has {count} rows")
        return True

    finally:
        conn.close()


def extract_cdc_callable(**context) -> dict:
    """
    Calls run_extraction() from cdc_extractor.py.
    Reads pipeline_mode from Airflow Variable.
    Pushes result dict to XCom so validate_extract can read it.
    """
    import sys

    sys.path.insert(0, "/opt/airflow/scripts")
    from cdc_extractor import run_extraction

    mode = Variable.get("pipeline_mode", default_var="incremental").lower()
    log.info(f"CDC extraction mode: {mode}")

    result = run_extraction(mode=mode)

    # XCom push — validate_extract and trigger_glue_job will read this
    context["ti"].xcom_push(key="cdc_result", value=result)
    log.info(f"XCom pushed: {json.dumps(result, default=str)}")

    # After a successful FULL load → automatically reset to incremental
    if mode == "full":
        Variable.set("pipeline_mode", "incremental")
        log.info("pipeline_mode reset to 'incremental' after full load")

    return result


def validate_extract_callable(**context) -> bool:
    ti = context["ti"]
    cdc_result = ti.xcom_pull(task_ids="extract_cdc", key="cdc_result")

    if not cdc_result:
        log.warning("No XCom result from extract_cdc — skipping downstream")
        return False

    rows_extracted = cdc_result.get("rows_extracted", 0)
    s3_uri = cdc_result.get("s3_uri")

    log.info(f"rows_extracted={rows_extracted}  s3_uri={s3_uri}")

    if rows_extracted == 0 or s3_uri is None:
        log.info("0 rows extracted — short-circuiting all downstream tasks")
        return False

    log.info(f"Validation passed — {rows_extracted} rows ready for Glue")
    return True


def refresh_data_quality_callable(**context) -> None:
    """
    Writes one summary row to pipeline_audit table in source Postgres.
    Power BI Pipeline Health page reads from this table.

    Columns:
      run_id          — Airflow run_id (unique per DAG run)
      run_date        — logical execution date
      curated_rows    — estimated from CDC extract count
      quarantine_rows — estimated ~10% of extracted rows
      dbt_test_status — 'passed' | 'unknown'
    """
    import os
    import psycopg2

    ti = context["ti"]
    run_id = context["run_id"]
    logical_date = context["logical_date"].date()

    cdc_result = ti.xcom_pull(task_ids="extract_cdc", key="cdc_result") or {}
    rows_extracted = cdc_result.get("rows_extracted", 0)

    # ~10% of extracted rows are dirty (matches seed_updates.py logic)
    quarantine_approx = max(0, rows_extracted // 10)
    curated_approx = rows_extracted - quarantine_approx

    # dbt success flag from BashOperator return value
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
                    (
                        run_id,
                        logical_date,
                        curated_approx,
                        quarantine_approx,
                        dbt_status,
                    ),
                )
        log.info("Audit row written ✓")
    finally:
        conn.close()


with DAG(
    dag_id="ecommerce_pipeline",
    default_args=DEFAULT_ARGS,
    description="StreamCart CDC pipeline: Postgres → S3 → Glue → dbt → Audit",
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
        script_args={
            "--S3_INPUT_PATH": (
                "{{ ti.xcom_pull(task_ids='extract_cdc', key='cdc_result')"
                "['s3_uri'] | default('') }}"
            ),
            "--PIPELINE_RUN_ID": "{{ run_id }}",
        },
        wait_for_completion=True,
        verbose=True,
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            set -e
            cd /opt/airflow/dbt

            PIPELINE_MODE="{{ var.value.pipeline_mode | default('incremental') }}"
            echo "Pipeline mode: $PIPELINE_MODE"

            if [ "$PIPELINE_MODE" = "incremental" ]; then
                echo "=== dbt run (tag:incremental only) ==="
                dbt run --select tag:incremental --profiles-dir /opt/airflow/dbt
            else
                echo "=== dbt run (all models) ==="
                dbt run --profiles-dir /opt/airflow/dbt
            fi

            echo "=== dbt test ==="
            dbt test --profiles-dir /opt/airflow/dbt
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
        >> run_dbt_models
        >> refresh_data_quality_summary
    )
