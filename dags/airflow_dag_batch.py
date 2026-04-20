"""
File        : airflow_dag_batch.py
Location    : dags/
Description : Airflow DAG that orchestrates the hourly batch pipeline by triggering
              two Databricks jobs in sequence — ingestion followed by transformation.

Schedule    : Hourly (@hourly)
Owner       : neha

Pipeline Flow:
    [trigger_fetcher_uploader] >> [trigger_transformations]

    Task 1 — trigger_fetcher_uploader:
        Runs Databricks job that fetches crypto prices from CoinGecko API
        and uploads raw JSON to S3 Bronze layer.

    Task 2 — trigger_transformations:
        Runs Databricks job that processes Bronze → Silver → Gold
        medallion transformations using PySpark and Delta Lake.

Dependencies:
    - apache-airflow-providers-databricks

Airflow Connection Required:
    - conn_id : databricks_default
    - conn_type: Databricks
    - host    : your-databricks-workspace-url
    - token   : your-databricks-personal-access-token

Warning:
    FETCHER_JOB_ID and TRANSFORM_JOB_ID must match your Databricks job IDs.
    Verify the databricks_default connection exists in Airflow before running.
"""



from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# ── Configuration ─────────────────────────────────────────────────────────────
FETCHER_JOB_ID   = "456608304761514"
TRANSFORM_JOB_ID = "295662145766571"
DATABRICKS_CONN  = "databricks_default"

# ── Default Arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner"           : "neha",
    "depends_on_past" : False,
    "email_on_failure": True,
    "email"           : ["1492neha@gmail.com"],
    "start_date"      : datetime(2024, 1, 1),
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}

# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id          = "crypto_databricks_pipeline",
    default_args    = default_args,
    description     = "Hourly orchestration of Databricks jobs for Crypto Lakehouse batch pipeline",
    schedule_interval = "@hourly",
    catchup         = False,
    tags            = ["crypto", "batch", "databricks", "lakehouse"],
) as dag:

    # ── Task 1: Ingestion ─────────────────────────────────────────────────────
    # Fetches crypto prices from CoinGecko API and uploads raw JSON to S3 Bronze
    ingest_task = DatabricksRunNowOperator(
        task_id          = "trigger_fetcher_uploader",
        databricks_conn_id = DATABRICKS_CONN,
        job_id           = FETCHER_JOB_ID,
    )

    # ── Task 2: Transformation ────────────────────────────────────────────────
    # Runs Bronze → Silver → Gold medallion transformations on Databricks
    transform_task = DatabricksRunNowOperator(
        task_id          = "trigger_transformations",
        databricks_conn_id = DATABRICKS_CONN,
        job_id           = TRANSFORM_JOB_ID,
    )

    # ── Pipeline Dependency ───────────────────────────────────────────────────
    ingest_task >> transform_task