"""
File        : gold_transformations.py
Location    : streaming_pipeline/include/medallion/gold/
Description : Spark Structured Streaming job that reads the Silver Delta table
              from S3 and produces three Gold layer Delta tables for analytics.

Input       : S3 Delta table — s3a://crypto-lakehouse-neha/silver/crypto_prices_clean

Output Tables:
    - gold/daily_trends       — 24-hour windowed price aggregates per coin
    - gold/price_performance  — 5-minute sliding window moving average and volatility
    - gold/latest_snapshot    — most recent price record per coin (overwrite upsert)

Transformation Details:
    1. Daily Trends     : 24-hour tumbling window — avg, max, min price and volume
    2. Price Performance: 5-min sliding window (1-min slide) — moving avg, volatility
    3. Latest Snapshot  : foreachBatch upsert — always holds the freshest price per coin

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
    Do not change checkpointLocation paths once streaming has started.
    awaitAnyTermination() stops all queries if any one of them fails.
"""


import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
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

SILVER_PATH      = f"{S3_BUCKET}/silver/crypto_prices_clean"
GOLD_TRENDS      = f"{S3_BUCKET}/gold/daily_trends"
GOLD_PERFORMANCE = f"{S3_BUCKET}/gold/price_performance"
GOLD_SNAPSHOT    = f"{S3_BUCKET}/gold/latest_snapshot"
CHECKPOINT_BASE  = f"{S3_BUCKET}/checkpoints"


# ── Spark Session ─────────────────────────────────────────────────────────────
print("INFO  : Initializing Spark session...")

builder = (
    SparkSession.builder
    .appName("Crypto_Gold_Streaming")
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


# ── Read Silver Stream ────────────────────────────────────────────────────────
print(f"INFO  : Reading Silver stream from: {SILVER_PATH}")

df_silver      = spark.readStream.format("delta").load(SILVER_PATH)
df_watermarked = df_silver.withWatermark("event_timestamp", "10 minutes")


# ── Gold Table 1: Daily Trends ────────────────────────────────────────────────
# 24-hour tumbling window — avg, max, min price and volume per coin
df_trends = (
    df_watermarked
    .groupBy(
        "coin_id",
        F.window("event_timestamp", "24 hours").alias("time_window")
    )
    .agg(
        F.avg("price_usd").alias("daily_avg_price"),
        F.max("price_usd").alias("daily_max_price"),
        F.min("price_usd").alias("daily_min_price"),
        F.avg("volume_24h").alias("daily_avg_volume"),
    )
    .select(
        "coin_id",
        F.col("time_window.start").alias("window_start"),
        "daily_avg_price",
        "daily_max_price",
        "daily_min_price",
        "daily_avg_volume",
    )
)


# ── Gold Table 2: Price Performance ──────────────────────────────────────────
# 5-minute sliding window (1-min slide) — moving average and volatility per coin
df_performance = (
    df_watermarked
    .groupBy(
        "coin_id",
        F.window("event_timestamp", "5 minutes", "1 minute").alias("perf_window")
    )
    .agg(
        F.avg("price_usd").alias("moving_avg_price"),
        F.stddev("price_usd").alias("price_volatility"),
    )
    .select(
        "coin_id",
        F.col("perf_window.start").alias("start_time"),
        "moving_avg_price",
        "price_volatility",
    )
)


# ── Gold Table 3: Latest Snapshot (foreachBatch upsert) ───────────────────────
# Keeps only the most recent price record per coin — overwrites on every batch
def upsert_latest_snapshot(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    window_spec     = Window.partitionBy("coin_id").orderBy(F.col("event_timestamp").desc())
    latest_in_batch = (
        batch_df
        .withColumn("rn", F.row_number().over(window_spec))
        .filter("rn = 1")
        .drop("rn")
    )
    latest_in_batch.write.format("delta").mode("overwrite").save(GOLD_SNAPSHOT)


# ── Write Gold Streams to S3 ──────────────────────────────────────────────────
print(f"INFO  : Starting Gold streams — writing to {S3_BUCKET}/gold/")
print("─" * 60)

query1 = (
    df_trends.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_trends")
    .start(GOLD_TRENDS)
)

query2 = (
    df_performance.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_perf")
    .start(GOLD_PERFORMANCE)
)

query3 = (
    df_silver.writeStream
    .foreachBatch(upsert_latest_snapshot)
    .option("checkpointLocation", f"{CHECKPOINT_BASE}/gold_snapshot")
    .start()
)

print("INFO  : All 3 Gold streams running — press Ctrl+C to stop")
print(f"INFO  : daily_trends      -> {GOLD_TRENDS}")
print(f"INFO  : price_performance -> {GOLD_PERFORMANCE}")
print(f"INFO  : latest_snapshot   -> {GOLD_SNAPSHOT}")

spark.streams.awaitAnyTermination()