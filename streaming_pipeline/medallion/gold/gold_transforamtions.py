import os
from pyspark.sql import SparkSession, functions as f

# 1. ENV SETUP
os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

spark = SparkSession.builder \
    .appName("Crypto_Gold_Streaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. READ FROM SILVER STREAM
silver_path = "A:/Crypto-Data-lakehouse-pipeline/data/silver/crypto_prices_clean"
df_silver = spark.readStream.format("delta").load(silver_path)

# 3. APPLY WATERMARK (Necessary for Windowing)
# We tell Spark to wait up to 10 minutes for late data
df_watermarked = df_silver.withWatermark("event_timestamp", "10 minutes")

# ---------------------------------------------------------
# 🎯 TRANSFORMATION: Daily Trends (Streaming Aggregation)
# ---------------------------------------------------------
# Streaming requires us to group by the window or the date
df_trends_stream = df_watermarked.groupBy(
    "coin_id", 
    f.window("event_timestamp", "24 hours").alias("time_window")
).agg(
    f.avg("price_usd").alias("daily_avg_price"),
    f.max("price_usd").alias("daily_max_price"),
    f.min("price_usd").alias("daily_min_price"),
    f.avg("volume_24h").alias("daily_avg_volume")
).select(
    "coin_id",
    f.col("time_window.start").alias("date"),
    "daily_avg_price",
    "daily_max_price",
    "daily_min_price",
    "daily_avg_volume"
)

# ---------------------------------------------------------
# 🎯 TRANSFORMATION: Performance (5-Minute Moving Average)
# ---------------------------------------------------------
df_perf_stream = df_watermarked.groupBy(
    "coin_id",
    f.window("event_timestamp", "5 minutes", "1 minute").alias("window")
).agg(
    f.avg("price_usd").alias("moving_avg_price"),
    f.stddev("price_usd").alias("price_volatility")
)

# ---------------------------------------------------------
# 🚀 WRITE TO GOLD TABLES
# ---------------------------------------------------------
gold_path_trends = "A:/Crypto-Data-lakehouse-pipeline/data/gold/daily_trends"
gold_path_perf = "A:/Crypto-Data-lakehouse-pipeline/data/gold/price_performance"

# Write Trends
query1 = df_trends_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "A:/Crypto-Data-lakehouse-pipeline/data/checkpoint/gold_trends") \
    .start(gold_path_trends)

# Write Performance
query2 = df_perf_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "A:/Crypto-Data-lakehouse-pipeline/data/checkpoint/gold_perf") \
    .start(gold_path_perf)

print("🏆 Gold Streams are running! Aggregating Trends and Performance...")
spark.streams.awaitAnyTermination()
