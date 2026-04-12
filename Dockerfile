FROM apache/airflow:2.7.1

USER root
RUN apt-get update && apt-get install -y build-essential

USER airflow
# We explicitly upgrade openlineage to bypass the error you are seeing
RUN pip install --no-cache-dir \
    "apache-airflow-providers-openlineage>=1.8.0" \
    "apache-airflow-providers-databricks" \
    "boto3"
