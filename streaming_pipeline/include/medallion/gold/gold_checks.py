"""
File        : gold_checks.py
Location    : streaming_pipeline/include/medallion/gold/
Description : Reads and inspects the three Gold layer Delta tables on S3
              to verify data was written correctly by gold_transformations.py.
              Provides a quick visual confirmation of all Gold outputs.

Tables Inspected:
    - s3a://crypto-lakehouse-neha/gold/price_performance
    - s3a://crypto-lakehouse-neha/gold/daily_trends
    - s3a://crypto-lakehouse-neha/gold/latest_snapshot

Dependencies:
    - pyspark==3.4.0
    - delta-spark==2.4.0
    - hadoop-aws==3.3.4

Environment Variables Required (.env):
    - JAVA_HOME
    - HADOOP_HOME
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - S3_BUCKET          (default: s3a://crypto-lakehouse-neha)

Warning:
    Never hardcode AWS credentials. Always load from environment variables.
    Run gold_transformations.py before this file — Gold tables must exist on S3.
"""


import os
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip
from dotenv import load_dotenv

load_dotenv()


# ── Environment Setup ─────────────────────────────────────────────────────────
JAVA_HOME   = os.getenv("JAVA_HOME",   r"C:\Program Files\Amazon Corretto\jdk11.0.30_7")
HADOOP_HOME = os.getenv("HADOOP_HOME", r"C:\hadoop")

os.environ["JAVA_HOME"]   = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = (
    os.path.join(JAVA_HOME,   "bin") + os.pathsep +
    os.path.join(HADOOP_HOME, "bin") + os.pathsep +
    os.environ["PATH"]
)

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.200 pyspark-shell"
)


# ── Configuration ─────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET             = os.getenv("S3_BUCKET", "s3a://crypto-lakehouse-neha")

SILVER_PATH   = f"{S3_BUCKET}/silver/crypto_prices_clean"
PATH_PERF     = f"{S3_BUCKET}/gold/price_performance"
PATH_TRENDS   = f"{S3_BUCKET}/gold/daily_trends"
PATH_SNAPSHOT = f"{S3_BUCKET}/gold/latest_snapshot"


# ── Spark Session ─────────────────────────────────────────────────────────────
print("INFO  : Initializing Spark session...")

builder = (
    SparkSession.builder
    .appName("Gold_Quality_Checks")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.access.key",
            AWS_ACCESS_KEY_ID)
    .config("spark.hadoop.fs.s3a.secret.key",
            AWS_SECRET_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.endpoint",
            "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access",      "false")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.sql.shuffle.partitions",               "2")
    .master("local[*]")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# ── Load Gold Tables ──────────────────────────────────────────────────────────
print("INFO  : Loading Gold tables from S3...")

df_gold_perf   = spark.read.format("delta").load(PATH_PERF)
df_gold_trends = spark.read.format("delta").load(PATH_TRENDS)
df_gold_snap   = spark.read.format("delta").load(PATH_SNAPSHOT)

print("─" * 60)
print("  GOLD LAYER: TABLE INSPECTION")
print("─" * 60)


# ── Price Performance ─────────────────────────────────────────────────────────
print("\n[TABLE 1] Price Performance")
print(f"  INFO     : Row count : {df_gold_perf.count()}")
print(f"  INFO     : Schema")
df_gold_perf.printSchema()
print("  INFO     : Latest 5 records")
df_gold_perf.orderBy(F.col("start_time").desc()).show(5, truncate=False)


# ── Daily Trends ──────────────────────────────────────────────────────────────
print("\n[TABLE 2] Daily Trends")
print(f"  INFO     : Row count : {df_gold_trends.count()}")
print(f"  INFO     : Schema")
df_gold_trends.printSchema()
print("  INFO     : Latest 5 records")
df_gold_trends.orderBy(F.col("window_start").desc()).show(5, truncate=False)


# ── Latest Snapshot ───────────────────────────────────────────────────────────
print("\n[TABLE 3] Latest Snapshot")
print(f"  INFO     : Row count : {df_gold_snap.count()}")
print(f"  INFO     : Schema")
df_gold_snap.printSchema()
print("  INFO     : All records (should be 3 — one per coin)")
df_gold_snap.orderBy("coin_id").show(truncate=False)

print("\n" + "─" * 60)
print("  GOLD CHECKS COMPLETE")
print("─" * 60)