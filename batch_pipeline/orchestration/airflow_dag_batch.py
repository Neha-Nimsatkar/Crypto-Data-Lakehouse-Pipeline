from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG('databricks_dag', start_date=datetime(2023, 1, 1), schedule_interval=None, catchup=False) as dag:
    
    run_job = DatabricksRunNowOperator(
        task_id='run_my_databricks_job',
        databricks_conn_id='databricks_default',
        job_id=294840511200667  # Replace with your actual Databricks Job ID
    )
