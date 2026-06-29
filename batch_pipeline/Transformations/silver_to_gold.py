# Transforms Silver layer data into three Gold layer Delta tables 
# optimised for business intelligence and analytics consumption.



from pyspark.sql import functions as F
from pyspark.sql.window import Window


BUCKET_NAME = "crypto-lakehouse-nehaa"

SILVER_TABLE = "workspace.default.silver_crypto_prices"
GOLD_SNAPSHOT = "workspace.default.gold_latest_snapshot"
GOLD_PERFORMANCE = "workspace.default.gold_price_performance"
GOLD_DAILY_TRENDS = "workspace.default.gold_daily_trends"


PATH_GOLD_SNAPSHOT = f"s3a://{BUCKET_NAME}/gold/latest_snapshot"
PATH_GOLD_PERFORMANCE = f"s3a://{BUCKET_NAME}/gold/price_performance"
PATH_GOLD_DAILY_TRENDS = f"s3a://{BUCKET_NAME}/gold/daily_trends"

MOVING_AVG_WINDOW = 7

df_silver = spark.read.table(SILVER_TABLE)


print("gold layer transformation begins..")



# Table 1: Latest Snapshot

print("\n Table 1 - Building gold_latest_snapshot...")

latest_window = Window.partitionBy("coin_id").orderBy(F.col("event_timestamp").desc())

df_snapshot = (
    df_silver
    .withColumn("row_num", F.row_number().over(latest_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
    .select(
        "coin_id",
        "price_usd",
        "market_cap",
        "volume_24h",
        "event_timestamp"
    )
)
(
    df_snapshot.write
    .format("delta")
    .mode("overwrite")
    .option("path", PATH_GOLD_SNAPSHOT)
    .option("mergeSchema", "true")
    .saveAsTable(GOLD_SNAPSHOT)
)
print(f"written to {GOLD_SNAPSHOT}")



# Table 2: Price Performance

print("\n Table 2 - Building gold_price_performance...")

perf_window = Window.partitionBy("coin_id").orderBy("event_timestamp").rowsBetween(-(MOVING_AVG_WINDOW - 1), 0)
ranking_window = Window.partitionBy("event_timestamp").orderBy(F.col("market_cap").desc())

df_performance = (
    df_silver
    .withColumn("moving_avg_price", F.avg("price_usd").over(perf_window))
    .withColumn("raw_volatility",   F.stddev("price_usd").over(perf_window))
    .withColumn("price_volatility", F.coalesce(F.col("raw_volatility"), F.lit(0.0)))
    .withColumn("market_cap_rank",   F.rank().over(ranking_window))
    .select(
        "coin_id",
        "event_timestamp",
        "price_usd",
        "market_cap",
        "volume_24h",
        F.round("moving_avg_price", 4).alias("moving_avg_price"),
        F.round("price_volatility", 6).alias("price_volatility"), 
        "market_cap_rank",
        "date"
    )
)
(
    df_performance.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .option("path", PATH_GOLD_PERFORMANCE)
    .option("mergeSchema", "true")
    .saveAsTable(GOLD_PERFORMANCE)
)
print(f" written to {GOLD_PERFORMANCE}")




# Table 3: Daily Trends

print("\n Table 3 - Building gold_daily_trends...")

df_daily = (
    df_silver
    .groupBy("date", "coin_id")
    .agg(
        F.round(F.avg("price_usd"), 4).alias("daily_avg_price"),
        F.round(F.max("price_usd"), 4).alias("daily_max_price"),
        F.round(F.min("price_usd"), 4).alias("daily_min_price"),
        F.round(F.avg("volume_24h"), 2).alias("daily_avg_volume"),
        F.count("*").alias("record_count")
    )
    .orderBy(F.col("date").desc(), F.col("daily_avg_price").desc())
)
(
    df_daily.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .option("path", PATH_GOLD_DAILY_TRENDS)
    .option("mergeSchema", "true")
    .saveAsTable(GOLD_DAILY_TRENDS)
)
print(f" written to {GOLD_DAILY_TRENDS}")



print("gold transformations completed..")