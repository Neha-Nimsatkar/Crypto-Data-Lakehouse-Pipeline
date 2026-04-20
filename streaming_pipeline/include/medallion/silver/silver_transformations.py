"""
File        : silver_transformations.py
Location    : streaming_pipeline/include/medallion/silver/
Description : Spark Structured Streaming job that reads raw Bronze Delta table,
              normalises nested coin data into a flat row-per-coin structure,
              and writes the cleaned stream to S3 Silver Delta table.

Input       : Local Delta table  — data/bronze/crypto_prices_delta
Output      : S3 Delta table     — s3a://crypto-lakehouse-neha/silver/crypto_prices_clean

Transformation Steps:
    1. Reads streaming Bronze Delta table
    2. Adds fallback Unix timestamp for records missing last_updated_at
    3. Unpivots nested coin objects (bitcoin/ethereum/solana) into rows
       using Spark stack() expression
    4. Casts and cleans all fields — price, market cap, volume, timestamps
    5. Filters out NULL prices
    6. Applies 10-minute watermark on event_timestamp for late data handling
    7. Writes append-mode stream to S3 Silver Delta table

Dependencies:
    - pyspark==3.4.0
    - delta-spark==2.4.0
    - hadoop-aws==3.3.4

Environment Variables Required (.env):
    - JAVA_HOME          (default: C:/Program Files/Amazon Corretto/jdk11.0.30_7)
    - HADOOP_HOME        (default: C:/hadoop)
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - S3_BUCKET          (default: s3a://crypto-lakehouse-neha)
    - BRONZE_PATH        (default: data/bronze/crypto_prices_delta)

Warning:
    Never hardcode AWS credentials. Always load from environment variables.
    Do not change checkpointLocation once streaming has started —
    Spark uses it to track processed offsets.
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
COINS      = ["bitcoin", "ethereum", "solana"]
S3_BUCKET  = os.getenv("S3_BUCKET", "s3a://crypto-lakehouse-neha")

BRONZE_PATH       = os.getenv("BRONZE_PATH", "data/bronze/crypto_prices_delta")
SILVER_PATH       = f"{S3_BUCKET}/silver/crypto_prices_clean"
CHECKPOINT_SILVER = f"{S3_BUCKET}/checkpoints/silver"


# ── Spark Session ─────────────────────────────────────────────────────────────
print("INFO  : Initializing Spark session...")

builder = (
    SparkSession.builder
    .appName("Crypto_Silver_Streaming")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint",
            "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access",   "false")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.sql.shuffle.partitions",            "2")
    .master("local[*]")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")


# ── Read Bronze Stream ────────────────────────────────────────────────────────
print(f"INFO  : Reading Bronze Delta stream from: {BRONZE_PATH}")

df_stream = spark.readStream.format("delta").load(BRONZE_PATH)


# ── Transformation ────────────────────────────────────────────────────────────
# Add fallback Unix timestamp for records missing last_updated_at
df_with_time = df_stream.withColumn("fallback_time", F.unix_timestamp())


# Build stack() expression to unpivot nested coin objects into rows
stack_parts = []
for coin in COINS:
    price   = f"CAST({coin}.usd AS DOUBLE)"
    m_cap   = f"COALESCE(CAST({coin}.usd_market_cap AS DOUBLE), 0.0)"
    vol     = f"COALESCE(CAST({coin}.usd_24h_vol AS DOUBLE), 0.0)"
    updated = f"COALESCE(CAST({coin}.last_updated_at AS LONG), fallback_time)"
    stack_parts.append(f"'{coin}', {price}, {m_cap}, {vol}, {updated}")

stack_expr = ", ".join(stack_parts)

df_normalized = df_with_time.select(
    F.expr(
        f"stack({len(COINS)}, {stack_expr}) "
        f"as (coin_id, price_usd, market_cap, volume_24h, updated_at)"
    ),
    F.col("ingestion_metadata.ingested_at").alias("ingested_at_str"),
)

df_cleaned = (
    df_normalized
    .select(
        "coin_id",
        "price_usd",
        "market_cap",
        "volume_24h",
        F.to_timestamp(
            F.from_unixtime(F.col("updated_at"))
        ).alias("event_timestamp"),
        F.to_timestamp(F.col("ingested_at_str")).alias("ingested_at"),
    )
    .filter(F.col("price_usd").isNotNull())
    .withWatermark("event_timestamp", "10 minutes")
)


# ── Write to S3 Silver ────────────────────────────────────────────────────────
print(f"INFO  : Starting Silver stream")
print(f"INFO  : Output path  : {SILVER_PATH}")
print(f"INFO  : Checkpoint   : {CHECKPOINT_SILVER}")
print("─" * 60)

query = (
    df_cleaned.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .start(SILVER_PATH)
)

print("INFO  : Silver stream running — press Ctrl+C to stop")
query.awaitTermination()