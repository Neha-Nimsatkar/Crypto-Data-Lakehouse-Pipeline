from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    dag_id='crypto_full_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # 1. First Task: Runs the Ingestion Job (Fetcher + Uploader)
    run_ingestion = DatabricksRunNowOperator(
        task_id='ingest_to_s3',
        databricks_conn_id='databricks_default',
        job_id=456608304761514  # Use the first ID here
    )

    # 2. Second Task: Runs the Transformation Job (Silver + Gold)
    run_transformation = DatabricksRunNowOperator(
        task_id='transform_and_load',
        databricks_conn_id='databricks_default',
        job_id=295662145766571 # Use the second ID here
    )

    # THIS IS THE CONNECTION
    # It tells Airflow: Run Ingestion first, and ONLY if it succeeds, run Transformation.
    run_ingestion >> run_transformation
