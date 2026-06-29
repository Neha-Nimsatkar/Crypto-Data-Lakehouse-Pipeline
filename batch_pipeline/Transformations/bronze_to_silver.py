# reads bronze JSON from S3, flattens coin data, and writes to silver delta table
# runs on Databricks — needs active SparkSession and S3 access

import os
import urllib.parse
from pyspark.sql import functions as F
from pyspark.sql.window import Window


try:
    from databricks.sdk.runtime import dbutils
    AWS_ACCESS_KEY = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="aws_id")
    AWS_SECRET_KEY = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="aws_secret")
except Exception as vault_err:
    print("dbutils not available, trying environment variables...")
    AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")


BUCKET_NAME = "crypto-lakehouse-nehaa"
BRONZE_PATH            = f"s3://{BUCKET_NAME}/bronze/*.json"
PRODUCTION_SILVER_PATH = f"s3://{BUCKET_NAME}/silver/crypto_prices"
CATALOG = "workspace"
SCHEMA = "default"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_crypto_prices"

EXPECTED_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano", "dogecoin",
    "polkadot", "polygon", "shiba-inu", "avalanche-2", "chainlink",
    "uniswap", "litecoin", "stellar", "near"
]


print("silver transformations start")


df_raw = (spark.read
          .option("multiLine", "true")
          .option("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY)
          .option("fs.s3.awsSecretAccessKey", AWS_SECRET_KEY)
          .json(BRONZE_PATH))
print(f"\n bronze data loaded securely")

ingested_at_col = F.col("ingestion_metadata.ingested_at").alias("ingested_at_str")

all_columns = df_raw.columns

select_exprs = []
if "ingestion_metadata" in all_columns:
    select_exprs.append(ingested_at_col)
else:
    select_exprs.append(F.lit(None).cast("string").alias("ingested_at_str"))

for coin in EXPECTED_COINS:
    if coin in all_columns:
        select_exprs.append(F.col(f"`{coin}`"))
    else:
        null_struct = F.struct(
            F.lit(None).cast("double").alias("usd"),
            F.lit(None).cast("double").alias("usd_market_cap"),
            F.lit(None).cast("double").alias("usd_24h_vol"),
            F.lit(None).cast("long").alias("last_updated_at")
        )
        select_exprs.append(null_struct.alias(coin))

df_with_meta = df_raw.select(*select_exprs)

print(f"found {len(EXPECTED_COINS)} coins in bronze data")
print(f"coins: {EXPECTED_COINS}")


# Explode dynamic coin columns maps into clean rows
df_exploded = df_with_meta.select(
    "ingested_at_str",
    F.explode(
        F.create_map(*[item for coin in EXPECTED_COINS for item in (F.lit(coin), F.col(coin))])
    ).alias("coin_id", "data")
).filter(F.col("data").isNotNull())

print(f"JSON dynamically flattened ..")


# Clean and Cast Columns
df_cleaned = df_exploded.select(
    F.lower(F.col("coin_id")).alias("coin_id"),
    F.col("data.usd").cast("double").alias("price_usd"),
    F.col("data.usd_market_cap").cast("double").alias("market_cap"),
    F.col("data.usd_24h_vol").cast("double").alias("volume_24h"),
    F.col("data.last_updated_at").alias("api_last_updated_at"),
    F.to_timestamp(F.col("ingested_at_str")).alias("ingested_at"),
).filter(F.col("price_usd").isNotNull())
print(f" cleaning and casting successfull")


# Timestamps and Partitioning Columns
df_transformed = (
    df_cleaned
    .withColumn("event_timestamp", F.to_timestamp(F.from_unixtime(F.col("api_last_updated_at"))))
    .withColumn("date", F.to_date(F.col("event_timestamp")))
    .withColumn("hour", F.hour(F.col("event_timestamp")))
)
print("timestamps extracted with date and hour columns added")


# Derived Metrics 
df_metrics = df_transformed.withColumn(
    "ingestion_delay_seconds",
    F.unix_timestamp(F.col("ingested_at")) - F.col("api_last_updated_at")
)

price_window = Window.partitionBy("coin_id").orderBy("event_timestamp")

df_flags = (
    df_metrics
    .withColumn("prev_price", F.lag("price_usd").over(price_window))
    .withColumn(
        "price_change_flag",
        F.when(F.col("prev_price").isNull(), "STABLE")
         .when(F.col("price_usd") > F.col("prev_price"), "UP")
         .when(F.col("price_usd") < F.col("prev_price"), "DOWN")
         .otherwise("STABLE")
    )
)
print("derived metrics added — ingestion delay and price change flag")



# Deduplication
dedup_window = Window.partitionBy("coin_id", "event_timestamp").orderBy(F.col("ingested_at").desc())

df_deduped = (
    df_flags
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn", "prev_price")
)
print(f" deduplicated successfull..")



# Add Load Timestamp
df_final = df_deduped.withColumn("load_timestamp", F.current_timestamp())
print("load timestamp added")



spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"\n Writing to silver delta table: {SILVER_TABLE}")
print(f"s3 path: {PRODUCTION_SILVER_PATH}")


(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .option("fs.s3.awsAccessKeyId", AWS_ACCESS_KEY)
    .option("fs.s3.awsSecretAccessKey", AWS_SECRET_KEY)
    .option("path", PRODUCTION_SILVER_PATH)  # write to S3 directly
    .option("mergeSchema", "true") 
    .saveAsTable(SILVER_TABLE)
)


print(f" 16 Coins Silver data processed and cataloged ")