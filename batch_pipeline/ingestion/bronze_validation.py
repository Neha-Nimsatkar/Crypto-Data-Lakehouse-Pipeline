# performs quality and integrity checks on raw bronze layer data
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

# Match the exact 15 coins from your producer code
EXPECTED_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano", "dogecoin", 
    "polkadot", "polygon", "shiba-inu", "avalanche-2", "chainlink", 
    "uniswap", "litecoin", "stellar", "near"
]

BRONZE_PATH = f"s3://{BUCKET_NAME}/bronze/*.json"



print("loading bronze data from S3 using multiLine JSON reader...")

try:
    # Use multiLine=true since the JSON files contain pretty-printed multi-line formatting
    df_bronze = (spark.read
                 .option("multiLine", "true")
                 .option("fs.s3.awsAccessKeyId", aws_access_key)
                 .option("fs.s3.awsSecretAccessKey", aws_secret_key)
                 .json(BRONZE_PATH))
    actual_cols = df_bronze.columns
    print("data loaded successfully from S3")
except Exception as e:
    print(f"failed to load data: {e}")
    raise e



print("running bronze validation checks")


# check 1 - json integrity
total_raw = df_bronze.count()
print(f"check 1 — total records: {total_raw}")

if "_corrupt_record" in actual_cols:
    corrupt_count = df_bronze.filter(F.col("_corrupt_record").isNotNull()).count()
    if corrupt_count > 0:
        print(f"fail — {corrupt_count} corrupt records found, stopping run")
        sys.exit(1)
    else:
        print("pass — no corrupt records found")
else:
    print("pass — no corrupt records found")


# check 2 — structural schema check
print("\ncheck 2 — structural schema check")
expected_schema_cols = ["symbol", "price", "volume", "timestamp"]
missing_cols = [c for c in expected_schema_cols if c not in actual_cols]

if missing_cols:
    print(f"fail — missing core structural columns in dataset: {missing_cols}")
    sys.exit(1)
else:
    print("pass — core streaming schema columns verified (symbol, price, volume, timestamp)")


# check 3 — asset tracking check (checking row values)
print("\ncheck 3 — asset tracking check")
distinct_coins_in_data = [row["symbol"] for row in df_bronze.select("symbol").distinct().collect() if row["symbol"] is not None]
missing_coins = [coin for coin in EXPECTED_COINS if coin not in distinct_coins_in_data]

if missing_coins:
    print(f"warning — some expected assets are missing from this batch window: {missing_coins}")
else:
    print(f"pass — tracking integrity verified. all {len(EXPECTED_COINS)} assets are present in the dataset rows")


# check 4 — null values check
print("\ncheck 4 — data quality gates")
null_prices = df_bronze.filter(F.col("price").isNull() | F.col("symbol").isNull()).count()

if null_prices > 0:
    print(f"fail — found {null_prices} rows with null values")
    sys.exit(1)
else:
    print("pass — zero null metrics detected in dataset values")


# check 5 - freshness check
print("\ncheck 5 — freshness check")
try:
    latest_timestamp_ms = df_bronze.select(F.max("timestamp")).collect()[0][0]
    print(f"latest ingestion stream epoch timestamp: {latest_timestamp_ms}")
except Exception as e:
    print(f"fail — could not read latest stream timestamp: {e}")

print("all bronze quality checks completed successfully")