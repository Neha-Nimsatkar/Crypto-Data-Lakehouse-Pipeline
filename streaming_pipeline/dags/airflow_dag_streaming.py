from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 1. Configuration: How the Manager (Airflow) should behave
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 0, 
}

# 2. The DAG Definition
with DAG(
    'crypto_medallion_streaming',
    default_args=default_args,
    description='Manages Silver and Gold Streaming + Validation',
    schedule_interval=None, 
    catchup=False
) as dag:

    # TASK 1: Start the Silver Transformation Stream
    # Path is updated to the internal Docker 'include' folder
    silver_stream = BashOperator(
        task_id='run_silver_stream',
        bash_command='python /usr/local/airflow/include/medallion/silver/silver_transformations.py'
    )

    # TASK 2: Start the Gold Transformation Stream
    gold_stream = BashOperator(
        task_id='run_gold_stream',
        bash_command='python /usr/local/airflow/include/medallion/gold/gold_transformations.py'
    )

    # TASK 3: Validate Silver Data
    validate_silver = BashOperator(
        task_id='validate_silver_data',
        bash_command='python /usr/local/airflow/include/medallion/silver/silver_validation.py'
    )

    # TASK 4: Validate Gold Data
    validate_gold = BashOperator(
        task_id='validate_gold_data',
        bash_command='python /usr/local/airflow/include/medallion/gold/gold_validation.py'
    )

    # 3. Setting the Hierarchy
    silver_stream >> validate_silver
    gold_stream >> validate_gold