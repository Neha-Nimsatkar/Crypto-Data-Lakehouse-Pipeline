"""
File        : bronze_ingestion_notes.py
Location    : batch_pipeline/medallion/bronze/
Description : The Bronze ingestion and validation for the batch pipeline
              runs directly inside Databricks notebooks as part of the
              same Spark session.

              Bronze Layer Responsibilities:
                  - Reads raw JSON files from S3 Bronze layer
                  - Validates JSON integrity and schema contract
                  - Checks for NULL prices, metadata lineage and freshness
                  - Acts as quality gate before Silver transformation

              S3 Bronze Path:
                  s3://crypto-lakehouse-neha/bronze/*.json

              Actual Implementation:
                  The Bronze validation code is embedded at the start of
                  the Silver transformation notebook in Databricks.
                  See: batch_pipeline/medallion/silver/silver_transformations.py

              Why not a standalone script?
                  Databricks notebooks share a single SparkSession across
                  cells. Loading Bronze data and immediately transforming
                  it to Silver in the same session avoids the overhead of
                  creating multiple Spark sessions for the same job.
"""


# This file is intentionally a reference note.
# See silver_transformations.py for the Bronze loading and validation code.