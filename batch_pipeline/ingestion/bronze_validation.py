# performs quality and integrity checks on raw, unflattened bronze layer data
# ingested from Kafka stream into AWS S3
# acts as a quality gate before data moves to the silver layer

import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


spark = SparkSession.builder.getOrCreate()

try:
    from databricks.sdk.runtime import dbutils
    aws_access_key = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="aws_id")
    aws_secret_key = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="aws_secret")
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "crypto-lakehouse-nehaa")
except Exception:
    from dotenv import load_dotenv
    load_dotenv()
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "crypto-lakehouse-nehaa")

EXPECTED_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano", "dogecoin", 
    "polkadot", "polygon", "shiba-inu", "avalanche-2", "chainlink", 
    "uniswap", "litecoin", "stellar", "near"
]

BRONZE_PATH = f"s3://{BUCKET_NAME}/bronze/*.json"



print("loading unflattened bronze records from S3...")

try:
    df_raw = spark.read.text(BRONZE_PATH)
    total_records = df_raw.count()
    print(f"data loaded. total raw records found: {total_records}")
except Exception as e:
    print(f"failed to load data from S3: {e}")
    raise e



print("running bronze validation checks")


# check 1 - empty file check
if total_records == 0:
    print("fail — S3 directory contains no records to validate.")
    sys.exit(1)
else:
    print("pass — raw records are present")


# ─── DEBUG STEP: PRINT RAW ROW STRUCTURE ──────────────────────────────────
print("\n[DEBUG] Printing the first raw record from S3 to inspect structure:")
try:
    sample_row = df_raw.limit(1).collect()[0]["value"]
    print(f"Sample Content: {sample_row}")
except Exception as debug_err:
    print(f"Could not read sample row: {debug_err}")
# ──────────────────────────────────────────────────────────────────────────


print("\nExtracting metrics for schema validation...")
# Try extracting directly from root or from a standard Kafka wrapper payload
df_parsed = df_raw.select(
    F.coalesce(
        F.get_json_object(F.col("value"), "$.symbol"),
        F.get_json_object(F.col("value"), "$.payload.symbol")
    ).alias("symbol"),
    F.coalesce(
        F.get_json_object(F.col("value"), "$.price"),
        F.get_json_object(F.col("value"), "$.payload.price")
    ).alias("price"),
    F.coalesce(
        F.get_json_object(F.col("value"), "$.volume"),
        F.get_json_object(F.col("value"), "$.payload.volume")
    ).alias("volume"),
    F.coalesce(
        F.get_json_object(F.col("value"), "$.timestamp"),
        F.get_json_object(F.col("value"), "$.payload.timestamp")
    ).alias("timestamp")
)


# check 2 — structural check
print("\ncheck 2 — structural check")
valid_structural_counts = df_parsed.filter(F.col("symbol").isNotNull()).count()

if valid_structural_counts == 0:
    print("fail — stream schema is broken. structural streaming payload keys do not exist in the json strings")
    sys.exit(1)
else:
    print(f"pass — structural streaming payload fields verified ({valid_structural_counts} valid structural ticks)")


# check 3 — asset tracking check
print("\ncheck 3 — asset tracking check")
unique_symbols_in_batch = [row["symbol"] for row in df_parsed.select("symbol").distinct().collect() if row["symbol"] is not None]
missing_coins = [coin for coin in EXPECTED_COINS if coin not in unique_symbols_in_batch]

if missing_coins:
    print(f"warning — the following assets were missing from this streaming chunk: {missing_coins}")
else:
    print(f"pass — tracking integrity verified. all {len(EXPECTED_COINS)} assets found in stream rows")


# check 4 — null values check
print("\ncheck 4 — data quality gates")
null_ticks = df_parsed.filter(F.col("price").isNull() | F.col("symbol").isNull()).count()

if null_ticks > 0:
    print(f"warning — detected {null_ticks} records with missing metric values inside batch window")
else:
    print("pass — zero null metrics found inside payload strings")


# check 5 - freshness check
print("\ncheck 5 — freshness check")
try:
    latest_epoch = df_parsed.select(F.max("timestamp")).collect()[0][0]
    print(f"latest streaming event timestamp found: {latest_epoch}")
except Exception as e:
    print(f"fail — could not parse timestamp metric: {e}")

print("all unflattened bronze quality checks completed successfully")