from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

# --- CONFIGURATION ---
# Your specific Databricks Job IDs
FETCHER_JOB_ID = "456608304761514"
TRANSFORM_JOB_ID = "295662145766571"

default_args = {
    'owner': 'neha',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['1492neha@gmail.com'],
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'crypto_databricks_pipeline',
    default_args=default_args,
    description='Orchestrating Databricks Jobs for Crypto Lakehouse',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    # Task 1: Trigger Fetcher & Uploader Job
    ingest_task = DatabricksRunNowOperator(
        task_id='trigger_fetcher_uploader',
        databricks_conn_id='databricks_default', # Ensure this matches your Airflow Connection
        job_id=FETCHER_JOB_ID
    )

    # Task 2: Trigger Transformations (Bronze -> Silver -> Gold)
    transform_task = DatabricksRunNowOperator(
        task_id='trigger_transformations',
        databricks_conn_id='databricks_default',
        job_id=TRANSFORM_JOB_ID
    )

    # Define Dependency
    ingest_task >> transform_task