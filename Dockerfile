# File: ./Dockerfile
FROM apache/airflow:2.7.1
RUN pip install --no-cache-dir apache-airflow-providers-databricks boto3
