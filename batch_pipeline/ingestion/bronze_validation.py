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


EXPECTED_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano", "dogecoin",
    "polkadot", "polygon", "shiba-inu", "avalanche-2", "chainlink",
    "uniswap", "litecoin", "stellar", "near"
]

BRONZE_PATH = f"s3://{BUCKET_NAME}/bronze/*.json"



print("loading bronze data from S3 using multiLine JSON reader...")

try:
    df_bronze = (spark.read
                 .option("multiLine", "true")
                 .option("fs.s3.awsAccessKeyId", aws_access_key)
                 .option("fs.s3.awsSecretAccessKey", aws_secret_key)
                 .json(BRONZE_PATH))
    actual_cols = df_bronze.columns
    print("data loaded successfully from S3")
except Exception as e:
    print(f"failed to load data from S3: {e}")
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


# check 2 & 3 — dynamic schema tracking & completeness check
print("\ncheck 2 & 3 — checking coin columns availability")

missing_coins = [coin for coin in EXPECTED_COINS if coin not in actual_cols]

if missing_coins:
    print(f"warning — some coins were not returned by the API in this window: {missing_coins}")
else:
    print("pass — all 15 expected coin columns are present in the dataset schema")


# check 4 — null prices check 
print("\ncheck 4 — checking for null prices on available coins")

for coin in EXPECTED_COINS:
    if coin in actual_cols:
        null_price = df_bronze.filter(F.col(f"`{coin}`.usd").isNull()).count()
        if null_price > 0:
            print(f"warning — {coin} column arrived but has {null_price} null price records")
        else:
            print(f"pass — {coin} structure looks clean")
    else:
        print(f"skip — {coin} skipped from null analysis (column did not arrive in S3 data)")


# check 5 - metadata ingestion check
print("\ncheck 5 — metadata validation")

if "ingestion_metadata" in actual_cols:
    invalid_meta = df_bronze.filter(
        F.col("ingestion_metadata.ingested_at").isNull() |
        (F.col("ingestion_metadata.ingested_at") == "")
    ).count()

    if invalid_meta > 0:
        print(f"fail — {invalid_meta} records missing a timestamp wrapper, stopping run")
        sys.exit(1)
    else:
        print("pass — ingestion metadata timestamp verified")
else:
    print("ingestion_metadata column was not found in this batch payload")

print("all checked rules evaluated. bronze validation process finished successfully")