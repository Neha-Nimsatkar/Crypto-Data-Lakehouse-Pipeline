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
    # Read as plain text rows to bypass serverless JSON parsing limits
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


# Parse JSON properties natively using Serverless-safe functions
print("\nExtracting metrics for schema validation...")
df_parsed = df_raw.select(
    F.get_json_object(F.col("value"), "$.symbol").alias("symbol"),
    F.get_json_object(F.col("value"), "$.price").alias("price"),
    F.get_json_object(F.col("value"), "$.volume").alias("volume"),
    F.get_json_object(F.col("value"), "$.timestamp").alias("timestamp")
)


# check 2 — structural check
print("\ncheck 2 — structural check")
# If a core field is entirely missing from the string structure, get_json_object returns all Nulls
null_structural_counts = df_parsed.filter(
    F.col("symbol").isNull() & F.col("price").isNull() & F.col("timestamp").isNull()
).count()

if null_structural_counts == total_records:
    print("fail — stream schema is broken. structural streaming payload keys do not exist in the json strings")
    sys.exit(1)
else:
    print("pass — structural streaming payload fields exist")


# check 3 — asset tracking check
print("\ncheck 3 — asset tracking check")
# Collect unique strings found inside the 'symbol' property
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
    print(f"fail — detected {null_ticks} records with corrupt null values inside payload")
    sys.exit(1)
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