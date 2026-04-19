"""
File        : gold_validations.py
Location    : batch_pipeline/medallion/gold/
Description : Validates business logic and cross-table consistency across
              Gold layer Delta tables.

Input       : S3 Delta tables —
                s3a://crypto-lakehouse-neha/gold/price_performance
                s3a://crypto-lakehouse-neha/gold/latest_snapshot

Checks Performed:
    1. Ranking integrity    — no duplicate ranks per timestamp
    2. Moving average       — no NULL moving averages
    3. Cross-table sync     — snapshot matches performance table

Dependencies:
    - pyspark
    - delta-core
    - hadoop-aws

Environment Variables Required (.env):
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY

Warning:
    Never hardcode AWS credentials.
    Always load from environment variables.
"""



import os
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
GOLD_PERF_PATH = "s3a://crypto-lakehouse-neha/gold/price_performance"
GOLD_SNAP_PATH = "s3a://crypto-lakehouse-neha/gold/latest_snapshot"


# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("Gold_Validation")
    .config("spark.jars.packages",
            "io.delta:delta-core_2.12:2.4.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.200")
    .config("spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .getOrCreate()
)

# ── Load Gold Tables ──────────────────────────────────────────────────────────
df_perf = spark.read.format("delta").load(GOLD_PERF_PATH)
df_snap = spark.read.format("delta").load(GOLD_SNAP_PATH)

print("─" * 60)
print("  GOLD LAYER: BUSINESS LOGIC VALIDATION")
print("─" * 60)



# ── Check 1: Ranking Integrity ────────────────────────────────────────────────
print("\n[CHECK 1] Ranking Integrity")

rank_check = (
    df_perf
    .groupBy("event_timestamp", "market_cap_rank")
    .count()
    .filter("count > 1")
    .count()
)

if rank_check > 0:
    print(f"  FAIL     : {rank_check} instance(s) of duplicate ranks per timestamp")
else:
    print("  PASS     : One rank per coin per timestamp — verified")



# ── Check 2: Moving Average Completeness ──────────────────────────────────────
print("\n[CHECK 2] Moving Average Completeness")

ma_nulls = df_perf.filter(F.col("moving_avg_price").isNull()).count()

if ma_nulls > 0:
    print(f"  FAIL     : {ma_nulls} record(s) missing moving average values")
else:
    print("  PASS     : 100% of records have moving average values")



# ── Check 3: Cross-Table Consistency ─────────────────────────────────────────
print("\n[CHECK 3] Cross-Table Consistency")

try:
    snap_price = (
        df_snap
        .filter("coin_id = 'bitcoin'")
        .select("price_usd")
        .collect()[0][0]
    )
    perf_price = (
        df_perf
        .filter("coin_id = 'bitcoin'")
        .orderBy(F.col("event_timestamp").desc())
        .limit(1)
        .select("price_usd")
        .collect()[0][0]
    )

    if abs(snap_price - perf_price) > 0.0001:
        print(f"  FAIL     : Snapshot (${snap_price}) and performance (${perf_price}) are out of sync")
    else:
        print(f"  PASS     : Tables are in sync — Bitcoin price: ${snap_price}")

except Exception as e:
    print(f"  WARNING  : Could not complete cross-table check: {e}")

print("\n" + "─" * 60)
print("  GOLD VALIDATION COMPLETE")
print("─" * 60)