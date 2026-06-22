"""
File        : bronze_validation.py
Location    : batch_pipeline/medallion/bronze/
Description : Performs comprehensive quality and integrity checks on raw Bronze
              layer data ingested from CoinGecko API into AWS S3.
              Acts as a quality gate before data moves to the Silver layer.
"""

import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark Session (if not already running inside Databricks context)
spark = SparkSession.builder.getOrCreate()

# ── UPGRADE: Databricks Workflow Parameters Handling ─────────────────────────
try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    BUCKET_NAME = dbutils.widgets.get("s3_bucket_name")
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "crypto-lakehouse-nehaa")

# ── Expected Pipeline Metadata Configurations ───────────────────────────────
EXPECTED_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano", "dogecoin", 
    "polkadot", "polygon", "shiba-inu", "avalanche-2", "chainlink", 
    "uniswap", "litecoin", "stellar", "near"
]
EXPECTED_KEYS = EXPECTED_COINS + ["ingestion_metadata"]

# Secure Direct S3 path mapping utilizing underlying Cloud IAM Roles
SECURE_BRONZE_PATH = f"s3://{BUCKET_NAME}/bronze/*.json"

print("🔄 Handshake establish kar rahe hain securely via Cloud IAM integration...")

# Load Data 
try:
    df_bronze = spark.read.option("multiLine", "true").json(SECURE_BRONZE_PATH)
    actual_keys = df_bronze.columns
    print("✅ PASS: Handshake Successful! Data loaded from S3.")
except Exception as e:
    print(f"❌ Critical System Failure: {e}")
    raise e

print("─" * 60)
print("  BRONZE LAYER: COMPREHENSIVE QUALITY GATE (15 COINS SYSTEM)")
print("─" * 60)


# ── Check 1: System-Level JSON Integrity ──────────────────────────────────────
total_raw = df_bronze.count()
print(f"\n[CHECK 1] System Integrity")
print(f"  Total records in Bronze : {total_raw}")

if "_corrupt_record" in actual_keys:
    corrupt_count = df_bronze.filter(F.col("_corrupt_record").isNotNull()).count()
    print(f"  CRITICAL : Corrupt JSON structures intercepted: {corrupt_count}")
    if corrupt_count > 0:
        import sys; sys.exit(1) # Stop downstream run if data corrupt
else:
    print("  PASS     : No corrupt JSON records detected")


# ── Check 2: Schema Contract ──────────────────────────────────────────────────
print(f"\n[CHECK 2] Schema Contract")

missing_keys = [c for c in EXPECTED_KEYS if c not in actual_keys]
extra_keys   = [c for c in actual_keys if c not in EXPECTED_KEYS and c != "_corrupt_record"]

if missing_keys:
    print(f"  FAIL     : Schema Contract Drift! Missing keys: {missing_keys}")
    import sys; sys.exit(1)
else:
    print("  PASS     : Structural integrity intact. All 15 expected keys verified.")

if extra_keys:
    print(f"  ALERT    : Upstream API schema evolution detected! Extra fields: {extra_keys}")


# ── Check 3: Data Completeness ────────────────────────────────────────────────
print(f"\n[CHECK 3] Data Completeness")

for coin in EXPECTED_COINS:
    if coin in actual_keys:
        null_price = df_bronze.filter(F.col(f"`{coin}`.usd").isNull()).count()
        if null_price > 0:
            print(f"  FAIL     : {coin.capitalize()} contains {null_price} missing observation instances (NULL price)")
        else:
            print(f"  PASS     : {coin.capitalize()} — 100% data density achieved")


# ── Check 4: Metadata & Lineage ───────────────────────────────────────────────
print(f"\n[CHECK 4] Metadata & Lineage")

if "ingestion_metadata" in actual_keys:
    invalid_meta = df_bronze.filter(
        F.col("ingestion_metadata.ingested_at").isNull() |
        (F.col("ingestion_metadata.ingested_at") == "")
    ).count()

    if invalid_meta > 0:
        print(f"  FAIL     : Data Lineage broken. {invalid_meta} record(s) missing collection timestamp")
        import sys; sys.exit(1)
    else:
        print("  PASS     : Telemetry metadata validation successful")
else:
    print("  FAIL     : 'ingestion_metadata' lineage object missing entirely!")
    import sys; sys.exit(1)


# ── Check 5: Data Freshness ───────────────────────────────────────────────────
print(f"\n[CHECK 5] Data Freshness")

try:
    latest_ingestion = df_bronze.select(
        F.max("ingestion_metadata.ingested_at")
    ).collect()[0][0]
    print(f"  INFO     : Dynamic telemetry check — Latest ingestion stamp: {latest_ingestion}")
except Exception as e:
    print(f"  WARNING  : Operational matrix error during timestamp evaluation: {e}")

print("\n" + "─" * 60)
print("  BRONZE QUALITY GATE EXECUTION TERMINATED SUCCESSFULLY")
print("─" * 60)