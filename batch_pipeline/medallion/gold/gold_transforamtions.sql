"""
File        : gold_transformations.py
Location    : batch_pipeline/medallion/gold/
Description : Transforms Silver layer data into three Gold layer Delta tables
              optimised for business intelligence and analytics consumption.

Input       : Databricks table — workspace.default.silver_crypto_prices

Output Tables:
    - workspace.default.gold_latest_snapshot   — latest price per coin
    - workspace.default.gold_price_performance — moving avg, volatility, ranking
    - workspace.default.gold_daily_trends      — daily OHLC-style aggregates

Dependencies:
    - pyspark
    - Databricks workspace with silver_crypto_prices table

Warning:
    Requires active SparkSession and Databricks Unity Catalog access.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Configuration ─────────────────────────────────────────────────────────────
SILVER_TABLE       = "workspace.default.silver_crypto_prices"
GOLD_SNAPSHOT      = "workspace.default.gold_latest_snapshot"
GOLD_PERFORMANCE   = "workspace.default.gold_price_performance"
GOLD_DAILY_TRENDS  = "workspace.default.gold_daily_trends"
MOVING_AVG_WINDOW  = 7

# ── Load Silver Data ──────────────────────────────────────────────────────────
df_silver = spark.read.table(SILVER_TABLE)

print("─" * 60)
print("  GOLD LAYER: TRANSFORMATIONS")
print("─" * 60)

# ── Table 1: Latest Snapshot ──────────────────────────────────────────────────
# Most recent price, market cap and volume per coin
print("\n[TABLE 1] Building gold_latest_snapshot...")

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

df_snapshot.write.format("delta").mode("overwrite").saveAsTable(GOLD_SNAPSHOT)
print(f"  DONE     : {df_snapshot.count()} coins written to {GOLD_SNAPSHOT}")

# ── Table 2: Price Performance ─────────────────────────────────────────────────
# Moving average, volatility and market cap rank per coin per timestamp
print("\n[TABLE 2] Building gold_price_performance...")

perf_window    = Window.partitionBy("coin_id").orderBy("event_timestamp").rowsBetween(-(MOVING_AVG_WINDOW - 1), 0)
ranking_window = Window.partitionBy("event_timestamp").orderBy(F.col("market_cap").desc())

df_performance = (
    df_silver
    .withColumn("moving_avg_price",  F.avg("price_usd").over(perf_window))
    .withColumn("price_volatility",  F.stddev("price_usd").over(perf_window))
    .withColumn("market_cap_rank",   F.rank().over(ranking_window))
    .select(
        "coin_id",
        "event_timestamp",
        "price_usd",
        "market_cap",
        "volume_24h",
        F.round("moving_avg_price", 4).alias("moving_avg_price"),
        F.round("price_volatility",  4).alias("price_volatility"),
        "market_cap_rank"
    )
)

df_performance.write.format("delta").mode("overwrite").saveAsTable(GOLD_PERFORMANCE)
print(f"  DONE     : {df_performance.count()} records written to {GOLD_PERFORMANCE}")

# ── Table 3: Daily Trends ─────────────────────────────────────────────────────
# Daily average, max and min price per coin
print("\n[TABLE 3] Building gold_daily_trends...")

df_daily = (
    df_silver
    .withColumn("date", F.to_date("event_timestamp"))
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

df_daily.write.format("delta").mode("overwrite").saveAsTable(GOLD_DAILY_TRENDS)
print(f"  DONE     : {df_daily.count()} records written to {GOLD_DAILY_TRENDS}")

print("\n" + "─" * 60)
print("  GOLD TRANSFORMATIONS COMPLETE")
print("─" * 60)