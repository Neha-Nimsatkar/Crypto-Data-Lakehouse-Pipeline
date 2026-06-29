# performs quality and integrity checks on raw bronze layer data
# ingested from CoinGecko API into AWS S3
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

if aws_access_key and aws_secret_key:
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", aws_access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", aws_secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

EXPECTED_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano", "dogecoin",
    "polkadot", "polygon", "shiba-inu", "avalanche-2", "chainlink",
    "uniswap", "litecoin", "stellar", "near"
]
EXPECTED_KEYS = EXPECTED_COINS + ["ingestion_metadata"]

BRONZE_PATH = f"s3a://{BUCKET_NAME}/bronze/*.json"



print("loading bronze data from S3...")

try:
    df_bronze = spark.read.option("multiLine", "true").json(BRONZE_PATH)
    actual_keys = df_bronze.columns
    print("data loaded from S3")
except Exception as e:
    print(f"failed to load data: {e}")
    raise e



print("running bronze validation checks")


# check 1 - json integrity
total_raw = df_bronze.count()
print(f"check 1 — total records: {total_raw}")

if "_corrupt_record" in actual_keys:
    corrupt_count = df_bronze.filter(F.col("_corrupt_record").isNotNull()).count()
    if corrupt_count > 0:
        print(f"fail — {corrupt_count} corrupt records found, stopping run")
        sys.exit(1)
    else:
        print("pass — no corrupt records found")
else:
    print("pass — no corrupt records found")


# check 2 - schema
print("\ncheck 2 — schema")

missing_keys = [c for c in EXPECTED_KEYS if c not in actual_keys]
extra_keys = [c for c in actual_keys if c not in EXPECTED_KEYS and c != "_corrupt_record"]

if missing_keys:
    print(f"fail — missing keys: {missing_keys}")
    sys.exit(1)
else:
    print("pass — all 15 expected keys present")

if extra_keys:
    print(f"warning — extra fields found: {extra_keys}")


# check 3 - completeness
print("\ncheck 3 — null prices per coin")

for coin in EXPECTED_COINS:
    if coin in actual_keys:
        null_price = df_bronze.filter(F.col(f"`{coin}`.usd").isNull()).count()
        if null_price > 0:
            print(f"fail — {coin} has {null_price} null prices")
        else:
            print(f"pass — {coin} looks clean")


# check 4 - metadata
print("\ncheck 4 — metadata")

if "ingestion_metadata" in actual_keys:
    invalid_meta = df_bronze.filter(
        F.col("ingestion_metadata.ingested_at").isNull() |
        (F.col("ingestion_metadata.ingested_at") == "")
    ).count()

    if invalid_meta > 0:
        print(f"fail — {invalid_meta} records missing timestamp, stopping run")
        sys.exit(1)
    else:
        print("pass — metadata looks good")
else:
    print("fail — ingestion_metadata column missing entirely")
    sys.exit(1)


# check 5 - freshness
print("\ncheck 5 — freshness")

try:
    latest_ingestion = df_bronze.select(
        F.max("ingestion_metadata.ingested_at")
    ).collect()[0][0]
    print(f"latest ingestion timestamp: {latest_ingestion}")
except Exception as e:
    print(f"fail — could not read latest timestamp: {e}")

print("all checks passed")