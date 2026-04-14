import os
from pyspark.sql import SparkSession, functions as f

# 1. ENV SETUP
os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

spark = SparkSession.builder \
    .appName("Gold_Streaming_Validation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. PATHS TO GOLD LOCAL DELTA TABLES
path_perf = "A:/Crypto-Data-lakehouse-pipeline/data/gold/price_performance"
path_trends = "A:/Crypto-Data-lakehouse-pipeline/data/gold/daily_trends"

# Load the tables from local storage
df_gold_perf = spark.read.format("delta").load(path_perf)
df_gold_trends = spark.read.format("delta").load(path_trends)

print("\n" + "="*50)
print("🏆 GOLD LAYER: STREAMING BUSINESS VALIDATION")
print("="*50)

# --- Check 1: Moving Average Calculation Accuracy ---
# In streaming, we check if the 5-minute moving average is populating
ma_nulls = df_gold_perf.filter(f.col("moving_avg_price").isNull()).count()
if ma_nulls > 0:
    print(f"❌ CALCULATION GAP: {ma_nulls} records are missing Moving Averages.")
else:
    print("✅ Analytics Completeness: 100% of records have Moving Averages.")

# --- Check 2: Windowing Integrity ---
# Ensure that for a single coin, we don't have overlapping window results
# (In streaming, the window column is a struct: window.start, window.end)
window_check = df_gold_perf.groupBy("coin_id", "window").count().filter("count > 1").count()
if window_check > 0:
    print(f"❌ LOGIC ERROR: Detected {window_check} duplicate window slots!")
else:
    print("✅ Windowing Integrity: Verified (Clean time-series slots).")

# --- Check 3: Cross-Layer Consistency ---
# We compare the Daily Average in Gold Trends with the raw average in Silver
# to ensure the aggregation hasn't dropped data.
silver_avg = spark.read.format("delta").load("A:/Crypto-Data-lakehouse-pipeline/data/silver/crypto_prices_clean") \
                  .filter("coin_id = 'bitcoin'") \
                  .select(f.avg("price_usd")).collect()[0][0]

gold_avg = df_gold_trends.filter("coin_id = 'bitcoin'") \
                         .select(f.avg("daily_avg_price")).collect()[0][0]

if gold_avg and silver_avg and abs(gold_avg - silver_avg) < 1.0:
    print("✅ Table Synchronization: Verified (Silver and Gold averages match).")
else:
    print(f"⚠️ SYNC WARNING: Variance detected between Silver ({silver_avg}) and Gold ({gold_avg}).")

# --- Check 4: Data Visualization ---
print("\n📊 Recent Analytics Snapshot (Performance Table):")
df_gold_perf.select("coin_id", "window.end", "moving_avg_price", "price_volatility") \
            .orderBy(f.col("window.end").desc()) \
            .show(5, truncate=False)

print("="*50 + "\n")