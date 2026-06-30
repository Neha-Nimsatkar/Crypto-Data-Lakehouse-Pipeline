# COMMAND ----------
# File: notebooks/04_gold_transformations.py
# Description: Real-Time Stream from Silver to 3 Gold Analytical Tables using forEachBatch
# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── CONFIGURATION & SECURE EXTERNAL S3 PATHS ──────────────────────────────────
BUCKET_NAME = "crypto-lakehouse-nehaa"

# Updating Target Paths to point explicitly to your AWS S3 Cloud Bucket
PATH_GOLD_SNAPSHOT    = f"s3a://{BUCKET_NAME}/gold_stream/latest_snapshot/"
PATH_GOLD_PERFORMANCE  = f"s3a://{BUCKET_NAME}/gold_stream/price_performance/"
PATH_GOLD_DAILY_TRENDS = f"s3a://{BUCKET_NAME}/gold_stream/daily_trends/"

# Defining the Metastore Table names for Delta Registration
GOLD_SNAPSHOT     = "workspace.default.gold_stream_latest_snapshot"
GOLD_PERFORMANCE  = "workspace.default.gold_stream_price_performance"
GOLD_DAILY_TRENDS = "workspace.default.gold_stream_daily_trends"

# Sliding window frame configuration for analytics 
MOVING_AVG_WINDOW = 7


# ── FOREACHBATCH COMPACT WRAPPER ENGINE ───────────────────────────────────────
def process_gold_tables(df_micro_batch, batch_id):
    if df_micro_batch.isEmpty():
        return

    print(f" Processing Micro-Batch ID: {batch_id} | Total Records: {df_micro_batch.count()}")

    # --- TABLE 1: LATEST SNAPSHOT ---
    latest_window = Window.partitionBy("coin_id").orderBy(F.col("event_timestamp").desc())
    df_snapshot = df_micro_batch \
        .withColumn("row_num", F.row_number().over(latest_window)) \
        .filter(F.col("row_num") == 1).drop("row_num") \
        .select("coin_id", "price_usd", "volume_24h", "event_timestamp")

    # In near real-time, snapshot is a state table, so we overwrite or merge it
    df_snapshot.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", PATH_GOLD_SNAPSHOT) \
        .saveAsTable(GOLD_SNAPSHOT)

    # --- TABLE 2: PRICE PERFORMANCE ---
    perf_window = Window.partitionBy("coin_id").orderBy("event_timestamp").rowsBetween(-(MOVING_AVG_WINDOW - 1), 0)
    ranking_window = Window.partitionBy("event_timestamp").orderBy(F.col("price_usd").desc())

    df_performance = df_micro_batch \
        .withColumn("moving_avg_price", F.avg("price_usd").over(perf_window)) \
        .withColumn("raw_volatility", F.stddev("price_usd").over(perf_window)) \
        .withColumn("price_volatility", F.coalesce(F.col("raw_volatility"), F.lit(0.0))) \
        .withColumn("market_cap_rank", F.rank().over(ranking_window)) \
        .select("coin_id", "event_timestamp", "price_usd", "volume_24h", 
                F.round("moving_avg_price", 4).alias("moving_avg_price"),
                F.round("price_volatility", 6).alias("price_volatility"), 
                "market_cap_rank", "date")

    df_performance.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("date") \
        .option("path", PATH_GOLD_PERFORMANCE) \
        .saveAsTable(GOLD_PERFORMANCE)

    # --- TABLE 3: DAILY TRENDS ---
    df_daily = df_micro_batch \
        .groupBy("date", "coin_id") \
        .agg(
            F.round(F.avg("price_usd"), 4).alias("daily_avg_price"),
            F.round(F.max("price_usd"), 4).alias("daily_max_price"),
            F.round(F.min("price_usd"), 4).alias("daily_min_price"),
            F.round(F.avg("volume_24h"), 2).alias("daily_avg_volume"),
            F.count("*").alias("record_count")
        )

    # Daily trends are incrementally merged or appended by date cluster
    df_daily.write \
        .format("delta") \
        .mode("append") \
        .partitionBy("date") \
        .option("path", PATH_GOLD_DAILY_TRENDS) \
        .saveAsTable(GOLD_DAILY_TRENDS)


# ── STEP 2: TRIGGER THE STREAMING PIPELINE ───────────────────────────────────
streaming_silver_df = spark.readStream.table("workspace.default.silver_crypto_prices")

GOLD_CHECKPOINT = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/gold_cascade_stream/"

query_gold = streaming_silver_df.writeStream \
    .foreachBatch(process_gold_tables) \
    .option("checkpointLocation", GOLD_CHECKPOINT) \
    .trigger(availableNow=True) \
    .start()

# ── MAGIC LINE: Enforces the script to wait until the execution stream safely ends ──
query_gold.awaitTermination()

print(" Gold Layer Cascade Complete! All 3 analytical targets updated successfully.")