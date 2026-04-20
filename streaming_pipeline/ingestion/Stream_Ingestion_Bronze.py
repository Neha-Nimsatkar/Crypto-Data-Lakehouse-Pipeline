"""
File        : Stream_Ingestion_Bronze.py
Location    : streaming_pipeline/ingestion/
Description : Spark Structured Streaming consumer that reads real-time cryptocurrency
              price messages from a Kafka topic, parses the JSON payload, and writes
              the raw data to a local Delta Lake Bronze table.

Input       : Kafka topic — crypto_prices
Output      : Delta Lake table — data/bronze/crypto_prices_delta

Schema (per message):
    bitcoin / ethereum / solana:
        usd             (double)  — price in USD
        usd_market_cap  (double)  — market capitalisation
        usd_24h_vol     (double)  — 24-hour trading volume
        last_updated_at (long)    — Unix timestamp of last update
    ingestion_metadata:
        source          (string)  — always 'CoinGecko API'
        ingested_at     (string)  — ISO timestamp of ingestion

Dependencies:
    - pyspark==3.4.0
    - delta-spark==2.4.0
    - Amazon Corretto JDK 11

Environment Variables Required (.env):
    - JAVA_HOME         (default: C:/Program Files/Amazon Corretto/jdk11.0.30_7)
    - HADOOP_HOME       (default: C:/hadoop)
    - KAFKA_BROKER      (default: localhost:9092)
    - KAFKA_TOPIC       (default: crypto_prices)
    - BRONZE_PATH       (default: data/bronze/crypto_prices_delta)
    - CHECKPOINT_PATH   (default: checkpoints/bronze_ingestion)

Usage:
    python Stream_Ingestion_Bronze.py

Warning:
    Requires Kafka broker running before starting this script.
    Java and Hadoop paths must be valid on the host machine.
    checkpoint_path must be consistent across restarts — do not change it
    once streaming has started or Spark will lose offset tracking.
"""


import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType
)
from dotenv import load_dotenv

load_dotenv()


# ── Environment Setup ─────────────────────────────────────────────────────────
JAVA_HOME    = os.getenv("JAVA_HOME",   r"C:\Program Files\Amazon Corretto\jdk11.0.30_7")
HADOOP_HOME  = os.getenv("HADOOP_HOME", r"C:\hadoop")

os.environ["JAVA_HOME"]   = JAVA_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = (
    os.path.join(JAVA_HOME,   "bin") + os.pathsep +
    os.path.join(HADOOP_HOME, "bin") + os.pathsep +
    os.environ["PATH"]
)

if not os.path.exists(JAVA_HOME):
    print(f"ERROR : Java path not found: {JAVA_HOME}")
    sys.exit(1)


# ── Configuration ─────────────────────────────────────────────────────────────
KAFKA_BROKER    = os.getenv("KAFKA_BROKER",    "localhost:9092")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",     "crypto_prices")
BRONZE_PATH     = os.getenv("BRONZE_PATH",     "data/bronze/crypto_prices_delta")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "checkpoints/bronze_ingestion")


# ── Schema Definition ─────────────────────────────────────────────────────────
coin_schema = StructType([
    StructField("usd",             DoubleType()),
    StructField("usd_market_cap",  DoubleType()),
    StructField("usd_24h_vol",     DoubleType()),
    StructField("last_updated_at", LongType()),
])

message_schema = StructType([
    StructField("bitcoin",  coin_schema),
    StructField("ethereum", coin_schema),
    StructField("solana",   coin_schema),
    StructField("ingestion_metadata", StructType([
        StructField("source",      StringType()),
        StructField("ingested_at", StringType()),
    ])),
])


# ── Spark Session ─────────────────────────────────────────────────────────────
print("INFO  : Initializing Spark session...")

spark = (
    SparkSession.builder
    .appName("CryptoStreamingBronze")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.hadoop.security.ignore.getSubject.error", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ── Read from Kafka ───────────────────────────────────────────────────────────
print(f"INFO  : Connecting to Kafka broker at {KAFKA_BROKER}")
print(f"INFO  : Subscribing to topic: {KAFKA_TOPIC}")

raw_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")
    .load()
)


# ── Parse JSON Payload ────────────────────────────────────────────────────────
parsed_df = (
    raw_stream_df
    .selectExpr("CAST(value AS STRING)")
    .select(F.from_json(F.col("value"), message_schema).alias("data"))
    .select("data.*")
)


# ── Write to Bronze Delta Table ───────────────────────────────────────────────
print(f"INFO  : Writing stream to Delta Bronze layer")
print(f"INFO  : Output path  : {BRONZE_PATH}")
print(f"INFO  : Checkpoint   : {CHECKPOINT_PATH}")
print("─" * 60)

query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema",        "true")
    .start(BRONZE_PATH)
)

print("INFO  : Streaming started — press Ctrl+C to stop")
query.awaitTermination()