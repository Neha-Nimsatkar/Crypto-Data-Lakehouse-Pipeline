"""
File        : gold_validations.py
Location    : streaming_pipeline/include/medallion/gold/
Description : Validates business logic and data integrity across all three
              Gold layer Delta tables on S3. Performs freshness, windowing,
              cross-layer sync, completeness and anomaly checks.

Input Tables:
    - s3a://crypto-lakehouse-neha/gold/price_performance
    - s3a://crypto-lakehouse-neha/gold/daily_trends
    - s3a://crypto-lakehouse-neha/gold/latest_snapshot
    - s3a://crypto-lakehouse-neha/silver/crypto_prices_clean

Checks Performed:
    Section A — Business Validation:
        1. Snapshot freshness     — latest record timestamp
        2. Windowing integrity    — no duplicate trend time slots
        3. Cross-layer sync       — Gold snapshot matches Silver head price
        4. Recent performance     — preview of latest moving averages

    Section B — Integrity Gate:
        5. Snapshot uniqueness    — one record per coin in snapshot
        6. Analytics completeness — no NULL moving averages or volatility
        7. Range reasonability    — Bitcoin daily avg within expected bounds
        8. Market snapshot preview

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
    Run gold_transformations.py before this file — Gold tables must exist.
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

BTC_DAILY_MIN = 20_000
BTC_DAILY_MAX = 150_000


# ── Spark Session ─────────────────────────────────────────────────────────────
print("INFO  : Initializing Spark session...")

builder = (
    SparkSession.builder
    .appName("Gold_Streaming_Validation")
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

df_perf     = spark.read.format("delta").load(PATH_PERF)
df_trends   = spark.read.format("delta").load(PATH_TRENDS)
df_snapshot = spark.read.format("delta").load(PATH_SNAPSHOT)


# ── Section A: Business Validation ───────────────────────────────────────────
print("\n" + "─" * 60)
print("  SECTION A — BUSINESS VALIDATION")
print("─" * 60)


# ── Check 1: Snapshot Freshness ───────────────────────────────────────────────
print("\n[CHECK 1] Snapshot Freshness")

latest_ts = df_snapshot.select(F.max("event_timestamp")).collect()[0][0]
print(f"  INFO     : Latest snapshot timestamp : {latest_ts}")


# ── Check 2: Windowing Integrity ──────────────────────────────────────────────
print("\n[CHECK 2] Windowing Integrity")

window_check = (
    df_trends
    .groupBy("coin_id", "window_start")
    .count()
    .filter("count > 1")
    .count()
)

if window_check > 0:
    print(f"  FAIL     : {window_check} duplicate trend time slot(s) detected")
else:
    print("  PASS     : No duplicate trend time slots")


# ── Check 3: Cross-Layer Snapshot Sync ───────────────────────────────────────
print("\n[CHECK 3] Cross-Layer Sync (Bitcoin)")

try:
    silver_latest = (
        spark.read.format("delta").load(SILVER_PATH)
        .filter("coin_id = 'bitcoin'")
        .orderBy(F.col("event_timestamp").desc())
        .select("price_usd")
        .first()[0]
    )
    gold_snap_price = (
        df_snapshot
        .filter("coin_id = 'bitcoin'")
        .select("price_usd")
        .first()[0]
    )

    if abs(silver_latest - gold_snap_price) < 0.01:
        print(f"  PASS     : Gold snapshot matches Silver head — ${gold_snap_price}")
    else:
        print(f"  WARNING  : Price mismatch — Silver: ${silver_latest} | Gold: ${gold_snap_price}")

except Exception as e:
    print(f"  WARNING  : Could not complete cross-layer sync check: {e}")


# ── Check 4: Recent Performance Preview ──────────────────────────────────────
print("\n[CHECK 4] Recent Performance (latest 5 moving averages)")

(
    df_perf
    .select("coin_id", "start_time", "moving_avg_price")
    .orderBy(F.col("start_time").desc())
    .show(5, truncate=False)
)


# ── Section B: Integrity Gate ─────────────────────────────────────────────────
print("\n" + "─" * 60)
print("  SECTION B — INTEGRITY GATE")
print("─" * 60)


# ── Check 5: Snapshot Uniqueness ──────────────────────────────────────────────
print("\n[CHECK 5] Snapshot Uniqueness")

snap_duplicates = (
    df_snapshot
    .groupBy("coin_id")
    .count()
    .filter("count > 1")
    .count()
)
total_coins = df_snapshot.count()

if snap_duplicates > 0:
    print(f"  FAIL     : Duplicate snapshot entries for {snap_duplicates} coin(s)")
else:
    print(f"  PASS     : Unique latest price for all {total_coins} coin(s)")


# ── Check 6: Analytics Completeness ──────────────────────────────────────────
print("\n[CHECK 6] Analytics Completeness")

null_metrics = df_perf.filter(
    F.col("moving_avg_price").isNull() | F.col("price_volatility").isNull()
).count()

if null_metrics > 0:
    print(f"  FAIL     : {null_metrics} record(s) missing moving average or volatility")
else:
    print("  PASS     : 100% of records have moving average and volatility values")


# ── Check 7: Range Reasonability ─────────────────────────────────────────────
print("\n[CHECK 7] Range Reasonability (Bitcoin daily avg)")

btc_anomalies = df_trends.filter(
    (F.col("coin_id") == "bitcoin") &
    ((F.col("daily_avg_price") < BTC_DAILY_MIN) |
     (F.col("daily_avg_price") > BTC_DAILY_MAX))
).count()

if btc_anomalies > 0:
    print(f"  FAIL     : {btc_anomalies} daily trend(s) outside expected range "
          f"({BTC_DAILY_MIN:,} – {BTC_DAILY_MAX:,})")
else:
    print(f"  PASS     : All Bitcoin daily averages within range "
          f"({BTC_DAILY_MIN:,} – {BTC_DAILY_MAX:,})")


# ── Check 8: Market Snapshot Preview ─────────────────────────────────────────
print("\n[CHECK 8] Current Market Snapshot")

df_snapshot.select(
    "coin_id", "price_usd", "event_timestamp"
).orderBy("coin_id").show(truncate=False)

print("\n" + "─" * 60)
print("  GOLD VALIDATION COMPLETE")
print("─" * 60)