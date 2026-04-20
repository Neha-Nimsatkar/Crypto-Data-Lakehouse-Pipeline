"""
File        : airflow_dag_streaming.py
Location    : streaming_pipeline/dags/
Description : Airflow DAG that orchestrates the streaming medallion pipeline.
              Triggers Silver and Gold PySpark streaming jobs and runs
              validation checks after each transformation completes.

Schedule    : Manual trigger only (schedule_interval=None)
Owner       : airflow

Pipeline Flow:
    [run_silver_stream] >> [validate_silver_data]
    [run_gold_stream]   >> [validate_gold_data]

    Note: Silver and Gold streams run in parallel.
          Each validation runs only after its respective stream completes.

Tasks:
    run_silver_stream    — Starts Silver transformation streaming job
    run_gold_stream      — Starts Gold transformation streaming job
    validate_silver_data — Runs Silver quality and integrity checks
    validate_gold_data   — Runs Gold business logic and integrity checks

Dependencies:
    - apache-airflow
    - apache-airflow-providers-standard (BashOperator)

Warning:
    Scripts are executed from the Docker container path /usr/local/airflow/include/.
    Ensure all medallion scripts exist at the correct paths inside the container
    before triggering this DAG.
    schedule_interval=None means this DAG only runs when triggered manually
    from the Airflow UI or via API.
"""


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Configuration ─────────────────────────────────────────────────────────────
INCLUDE_PATH = "/usr/local/airflow/include/medallion"


# ── Default Arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner"           : "airflow",
    "depends_on_past" : False,
    "start_date"      : datetime(2024, 1, 1),
    "email_on_failure": False,
    "retries"         : 0,
    "retry_delay"     : timedelta(minutes=5),
}


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id            = "crypto_medallion_streaming",
    default_args      = default_args,
    description       = "Orchestrates Silver and Gold streaming jobs and validations",
    schedule_interval = None,
    catchup           = False,
    tags              = ["crypto", "streaming", "medallion", "lakehouse"],
) as dag:


    # ── Task 1: Silver Transformation Stream ──────────────────────────────────
    # Reads Bronze Delta, normalises coin data, writes to S3 Silver Delta table
    run_silver_stream = BashOperator(
        task_id      = "run_silver_stream",
        bash_command = f"python {INCLUDE_PATH}/silver/silver_transformations.py",
    )


    # ── Task 2: Gold Transformation Stream ───────────────────────────────────
    # Reads Silver Delta, produces daily trends, performance and snapshot tables
    run_gold_stream = BashOperator(
        task_id      = "run_gold_stream",
        bash_command = f"python {INCLUDE_PATH}/gold/gold_transformations.py",
    )


    # ── Task 3: Silver Validation ─────────────────────────────────────────────
    # Runs integrity, freshness, volume and anomaly checks on Silver table
    validate_silver = BashOperator(
        task_id      = "validate_silver_data",
        bash_command = f"python {INCLUDE_PATH}/silver/silver_validation.py",
    )


    # ── Task 4: Gold Validation ───────────────────────────────────────────────
    # Runs business logic and integrity checks across all Gold tables
    validate_gold = BashOperator(
        task_id      = "validate_gold_data",
        bash_command = f"python {INCLUDE_PATH}/gold/gold_validations.py",
    )


    # ── Pipeline Dependencies ─────────────────────────────────────────────────
    run_silver_stream >> validate_silver
    run_gold_stream   >> validate_gold