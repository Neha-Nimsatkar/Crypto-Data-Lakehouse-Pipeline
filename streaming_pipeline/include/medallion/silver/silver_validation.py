"""
File        : silver_validation.py
Location    : streaming_pipeline/include/medallion/silver/
Description : Validates the streaming Silver Delta table on S3 after transformation
              from the Bronze layer. Performs integrity, freshness, volume,
              and anomaly checks before data is promoted to Gold.

Input       : S3 Delta table — s3a://crypto-lakehouse-neha/silver/crypto_prices_clean

Checks Performed:
    1. Integrity        — duplicate records and NULL price detection
    2. Coin coverage    — all three coins present in Silver
    3. Freshness        — age of latest record in minutes
    4. Volume           — row count per coin
    5. Anomaly          — impossible Bitcoin price range detection

Dependencies:
    - pyspark==3.4.0
    - delta-spark==2.4.0
    - hadoop-aws==3.3.4

Environment Variables Required (.env):
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - S3_BUCKET          (default: s3a://crypto-lakehouse-neha)

Warning:
    Never hardcode AWS credentials. Always load from environment variables.
    Ensure system timezone matches data timezone for accurate freshness checks.
"""


import os
import datetime
from pyspark.sql import SparkSession, functions as F
from dotenv import load_dotenv

load_dotenv()


# ── Configuration ─────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET             = os.getenv("S3_BUCKET", "s3a://crypto-lakehouse-neha")

SILVER_PATH           = f"{S3_BUCKET}/silver/crypto_prices_clean"
BITCOIN_PRICE_MIN     = 20_000
BITCOIN_PRICE_MAX     = 200_000


# ── Spark Session ─────────────────────────────────────────────────────────────
print("INFO  : Initializing Spark session...")

try:
    spark = (
        SparkSession.builder
        .appName("Silver_Validation_Streaming")
        .config("spark.jars.packages",
                "io.delta:delta-core_2.12:2.4.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.200")
        .config("spark.hadoop.fs.s3a.access.key",
                AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key",
                AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")


    # ── Load Silver Table ─────────────────────────────────────────────────────
    print(f"INFO  : Loading Silver table from: {SILVER_PATH}")
    df_check = spark.read.format("delta").load(SILVER_PATH)

    print("─" * 60)
    print("  SILVER LAYER: STREAMING VALIDATION")
    print("─" * 60)


    # ── Check 1: Integrity ────────────────────────────────────────────────────
    print("\n[CHECK 1] Integrity")

    duplicate_count = (
        df_check
        .groupBy("coin_id", "event_timestamp")
        .count()
        .filter("count > 1")
        .count()
    )
    null_prices = df_check.filter(F.col("price_usd").isNull()).count()

    print(f"  {'PASS' if duplicate_count == 0 else 'FAIL'}     : Duplicate records  : {duplicate_count}")
    print(f"  {'PASS' if null_prices == 0 else 'FAIL'}     : NULL price records : {null_prices}")


    # ── Check 2: Coin Coverage ────────────────────────────────────────────────
    print("\n[CHECK 2] Coin Coverage")

    coins_found = [
        row["coin_id"]
        for row in df_check.select("coin_id").distinct().collect()
    ]
    expected    = {"bitcoin", "ethereum", "solana"}
    missing     = expected - set(coins_found)

    print(f"  INFO     : Coins found  : {sorted(coins_found)}")
    if missing:
        print(f"  FAIL     : Missing coins : {sorted(missing)}")
    else:
        print(f"  PASS     : All 3 coins present")


    # ── Check 3: Freshness ────────────────────────────────────────────────────
    print("\n[CHECK 3] Freshness")

    latest_row = df_check.select(F.max("event_timestamp")).collect()[0][0]
    if latest_row:
        delay_mins = (
            datetime.datetime.now() - latest_row.replace(tzinfo=None)
        ).total_seconds() / 60
        status = "PASS" if delay_mins < 30 else "ALERT"
        print(f"  {status}     : Data is {round(delay_mins, 2)} minutes old")
    else:
        print("  WARNING  : No data found in Silver table yet")


    # ── Check 4: Volume ───────────────────────────────────────────────────────
    print("\n[CHECK 4] Volume")

    coin_counts = (
        df_check.groupBy("coin_id")
        .count()
        .orderBy("coin_id")
        .collect()
    )
    for row in coin_counts:
        print(f"  INFO     : {row['coin_id']:<12} — {row['count']} rows")


    # ── Check 5: Anomaly Detection ────────────────────────────────────────────
    print("\n[CHECK 5] Anomaly Detection")

    outliers = df_check.filter(
        (F.col("coin_id") == "bitcoin") &
        ((F.col("price_usd") < BITCOIN_PRICE_MIN) |
         (F.col("price_usd") > BITCOIN_PRICE_MAX))
    ).count()

    if outliers > 0:
        print(f"  FAIL     : {outliers} suspicious Bitcoin price point(s) detected")
    else:
        print(f"  PASS     : Bitcoin price range valid ({BITCOIN_PRICE_MIN:,} – {BITCOIN_PRICE_MAX:,})")


    # ── Sample Preview ────────────────────────────────────────────────────────
    print("\n[PREVIEW] Latest 5 records")
    df_check.orderBy(F.col("event_timestamp").desc()).show(5, truncate=False)

    print("\n" + "─" * 60)
    print("  SILVER VALIDATION COMPLETE")
    print("─" * 60)

except Exception as e:
    print(f"ERROR : Failed to load Silver data from S3: {e}")
    raise