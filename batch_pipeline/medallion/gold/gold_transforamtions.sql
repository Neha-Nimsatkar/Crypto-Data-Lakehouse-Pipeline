# Drop the existing managed Gold tables so we can redefine them with your S3 path
#1
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_latest_snapshot")
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_price_performance")
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_daily_trends")

# This clears any partial metadata that might be confusing Spark
#2
dbutils.fs.rm("s3://crypto-lakehouse-neha/gold/", True)


from pyspark.sql import functions as f
from pyspark.sql.window import Window

# 1. READ FROM THE SILVER TABLE
df_silver = spark.read.table("workspace.default.silver_crypto_prices")

# ---------------------------------------------------------
# 🎯 TRANSFORMATION 1: Latest Snapshot Logic
# ---------------------------------------------------------
latest_window = Window.partitionBy("coin_id").orderBy(f.col("event_timestamp").desc())

df_latest = df_silver.withColumn("rn", f.row_number().over(latest_window)) \
    .filter(f.col("rn") == 1) \
    .select(
        "coin_id", 
        "price_usd", 
        "market_cap", 
        "volume_24h", 
        "event_timestamp"
    )

# ---------------------------------------------------------
# 🎯 TRANSFORMATION 2: Price Performance Logic
# ---------------------------------------------------------
stats_window = Window.partitionBy("coin_id").orderBy("event_timestamp").rowsBetween(-10, 0)
rank_window = Window.partitionBy("event_timestamp").orderBy(f.col("market_cap").desc())

df_performance = df_silver.withColumn(
    "moving_avg_price", f.avg("price_usd").over(stats_window)
).withColumn(
    "price_volatility", f.stddev("price_usd").over(stats_window)
).withColumn(
    "market_cap_rank", f.rank().over(rank_window)
).select(
    "coin_id", 
    "event_timestamp", 
    "price_usd", 
    "moving_avg_price", 
    "price_volatility", 
    "market_cap_rank"
)

# ---------------------------------------------------------
# 🎯 TRANSFORMATION 3: Daily Trends Logic
# ---------------------------------------------------------
df_trends = df_silver.groupBy("coin_id", "date").agg(
    f.avg("price_usd").alias("daily_avg_price"),
    f.max("price_usd").alias("daily_max_price"),
    f.min("price_usd").alias("daily_min_price"),
    f.avg("volume_24h").alias("daily_avg_volume")
).withColumn("load_timestamp", f.current_timestamp())

# ---------------------------------------------------------
# 🚀 FINAL CLOUD SYNC: S3 EXTERNAL TABLE REGISTRATION
# ---------------------------------------------------------

# Define Paths
path_latest = "s3://crypto-lakehouse-neha/gold/latest_snapshot"
path_perf   = "s3://crypto-lakehouse-neha/gold/price_performance"
path_trends = "s3://crypto-lakehouse-neha/gold/daily_trends"

# 1. Physical Write to S3 (This bypasses the Metastore)
df_latest.write.format("delta").mode("overwrite").save(path_latest)
df_performance.write.format("delta").mode("overwrite").save(path_perf)
df_trends.write.format("delta").mode("overwrite").save(path_trends)

# 2. CLEAR METASTORE (This removes the "Already Exists" block)
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_latest_snapshot")
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_price_performance")
spark.sql("DROP TABLE IF EXISTS workspace.default.gold_daily_trends")

# 3. RE-REGISTER AS EXTERNAL TABLES
spark.sql(f"CREATE TABLE workspace.default.gold_latest_snapshot USING DELTA LOCATION '{path_latest}'")
spark.sql(f"CREATE TABLE workspace.default.gold_price_performance USING DELTA LOCATION '{path_perf}'")
spark.sql(f"CREATE TABLE workspace.default.gold_daily_trends USING DELTA LOCATION '{path_trends}'")

print(" SUCCESS: Gold Layer is now officially EXTERNAL and saved in your S3!")