# Drop the old tables to clear the metadata mismatch
# add karna ya nhi ye badme decide karenege 
spark.sql("DROP TABLE IF EXISTS workspace.default.silver_crypto_prices")
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_latest_snapshot")
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_price_performance")
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_daily_trends")


from pyspark.sql import functions as f
from pyspark.sql.functions import col, to_timestamp, current_timestamp, when, hour, to_date, unix_timestamp, lit, lower
from pyspark.sql.window import Window


# 1. READ RAW DATA WITH MULTILINE OPTION
# This fix resolves the _corrupt_record issue for pretty-printed JSON
bronze_path = "s3://crypto-lakehouse-neha/bronze/*.json"

df_raw = spark.read.option("multiLine", "true").json(bronze_path)


# 2. DYNAMIC FLATTENING
# We select the specific coin columns and the metadata separately
# This handles the nested structure and prepares it for the 'Explode'
df_with_meta = df_raw.select(
    f.col("ingestion_metadata.ingested_at").alias("ingested_at_str"),
    f.col("bitcoin"),
    f.col("ethereum"),
    f.col("solana")
)


# Convert nested JSON into rows (Tidy Data format)
df_exploded = df_with_meta.select(
    "ingested_at_str",
    f.explode(f.create_map(
        f.lit("bitcoin"), f.col("bitcoin"),
        f.lit("ethereum"), f.col("ethereum"),
        f.lit("solana"), f.col("solana")
    )).alias("coin_id", "data")
)


# 3. CLEANING & CASTING
df_cleaned = df_exploded.select(
    f.lower(f.col("coin_id")).alias("coin_id"),
    f.col("data.usd").cast("double").alias("price_usd"),
    f.col("data.usd_market_cap").cast("double").alias("market_cap"),
    f.col("data.usd_24h_vol").cast("double").alias("volume_24h"),
    f.col("data.last_updated_at").alias("api_last_updated_at"),
    f.to_timestamp(f.col("ingested_at_str")).alias("ingested_at")
).filter(f.col("price_usd").isNotNull())


# 4. TIMESTAMPS & PARTITIONING
# Convert Unix timestamp from API to Spark Timestamp
df_transformed = df_cleaned.withColumn(
    "event_timestamp", f.to_timestamp(f.from_unixtime(f.col("api_last_updated_at")))
).withColumn(
    "date", f.to_date(f.col("event_timestamp"))
).withColumn(
    "hour", f.hour(f.col("event_timestamp"))
)


# 5. ADVANCED DERIVED COLUMNS (Ingestion Delay & Price Flags)
# Metric: Delay between API update and our system ingestion
df_metrics = df_transformed.withColumn(
    "ingestion_delay_seconds", 
    f.unix_timestamp(f.col("ingested_at")) - f.col("api_last_updated_at")
)


# Window to compare current price with previous record
price_window = Window.partitionBy("coin_id").orderBy("event_timestamp")

df_flags = df_metrics.withColumn(
    "prev_price", f.lag("price_usd").over(price_window)
).withColumn(
    "price_change_flag", 
    f.when(f.col("price_usd") > f.col("prev_price"), "UP")
     .when(f.col("price_usd") < f.col("prev_price"), "DOWN")
     .otherwise("STABLE")
)


# 6. DEDUPLICATION
# Removes any duplicate API fetches for the same timestamp
dedup_window = Window.partitionBy("coin_id", "event_timestamp").orderBy(f.col("ingested_at").desc())

df_deduped = df_flags.withColumn("rn", f.row_number().over(dedup_window)) \
    .filter(f.col("rn") == 1) \
    .drop("rn", "prev_price")


# 7. FINAL STANDARDIZATION
df_final = df_deduped \
    .withColumn("load_timestamp", f.current_timestamp())


# 1.catalog name 
catalog_name = "workspace" 
schema_name = "default"

# 2. Create the schema if it doesn't exist (this ensures the 'folder' is there)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

# 3.Define your personal S3 destination for Silver
silver_cloud_path = "s3://crypto-lakehouse-neha/silver/crypto_prices"

# 4. WRITE TO SILVER TABLE
(df_final.write
  .format("delta")
  .mode("overwrite")
  .partitionBy("date")
  .option("path", silver_cloud_path) # This is the magic line that moves it to your S3
  .saveAsTable("workspace.default.silver_crypto_prices"))

print(f" Silver data successfully moved to cloud: {silver_cloud_path}")








