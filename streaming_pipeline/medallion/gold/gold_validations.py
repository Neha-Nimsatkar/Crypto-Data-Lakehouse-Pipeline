import os
from pyspark.sql import SparkSession, functions as f
from delta import configure_spark_with_delta_pip

os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

builder = SparkSession.builder \
    .appName("Gold_Streaming_Validation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

path_perf = "A:/Crypto-Data-lakehouse-pipeline/data/gold/price_performance"
path_trends = "A:/Crypto-Data-lakehouse-pipeline/data/gold/daily_trends"
path_snapshot = "A:/Crypto-Data-lakehouse-pipeline/data/gold/latest_snapshot"

df_gold_perf = spark.read.format("delta").load(path_perf)
df_gold_trends = spark.read.format("delta").load(path_trends)
df_gold_snap = spark.read.format("delta").load(path_snapshot)

print("\n" + "="*50)
print("🏆 GOLD LAYER: STREAMING BUSINESS VALIDATION")
print("="*50)

# --- Check 1: Snapshot Freshness ---
latest_ts = df_gold_snap.select(f.max("event_timestamp")).collect()[0][0]
print(f"✅ Data Freshness: Latest record timestamp is {latest_ts}")

# --- Check 2: Windowing Integrity (Using 'window_start' from our Gold Stream) ---
window_check = df_gold_trends.groupBy("coin_id", "window_start").count().filter("count > 1").count()
if window_check > 0:
    print(f"❌ LOGIC ERROR: Detected {window_check} duplicate trend slots!")
else:
    print("✅ Trend Windowing: Verified (Clean time-series slots).")

# --- Check 3: Cross-Layer Snapshot Sync ---
# The latest price in Silver should match the price in our Gold Snapshot
silver_latest = spark.read.format("delta").load("A:/Crypto-Data-lakehouse-pipeline/data/silver/crypto_prices_clean") \
                  .filter("coin_id = 'bitcoin'") \
                  .orderBy(f.col("event_timestamp").desc()).select("price_usd").first()[0]

gold_snap_price = df_gold_snap.filter("coin_id = 'bitcoin'").select("price_usd").first()[0]

if abs(silver_latest - gold_snap_price) < 0.01:
    print("✅ Snapshot Sync: Verified (Gold Snapshot matches Silver Head).")
else:
    print(f"⚠️ SYNC WARNING: Silver price ({silver_latest}) != Gold Snapshot ({gold_snap_price})")

# --- Check 4: Visualization ---
print("\n📊 RECENT PERFORMANCE (5-Min Moving Averages):")
df_gold_perf.select("coin_id", "start_time", "moving_avg_price") \
            .orderBy(f.col("start_time").desc()) \
            .show(5)

print("="*50 + "\n")