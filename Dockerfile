FROM apache/airflow:2.7.1

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential && \
    apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    "apache-airflow-providers-openlineage>=1.8.0" \
    "apache-airflow-providers-databricks" \
    "boto3" \
    "python-dotenv"