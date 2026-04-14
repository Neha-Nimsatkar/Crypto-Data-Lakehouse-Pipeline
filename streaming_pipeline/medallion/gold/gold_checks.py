import os
from pyspark.sql import SparkSession, functions as f
from delta import configure_spark_with_delta_pip

os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

builder = SparkSession.builder \
    .appName("Gold_Quality_Checks") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# PATHS
path_perf = "A:/Crypto-Data-lakehouse-pipeline/data/gold/price_performance"
path_trends = "A:/Crypto-Data-lakehouse-pipeline/data/gold/daily_trends"
path_snapshot = "A:/Crypto-Data-lakehouse-pipeline/data/gold/latest_snapshot"

# Load tables
df_perf = spark.read.format("delta").load(path_perf)
df_trends = spark.read.format("delta").load(path_trends)
df_snapshot = spark.read.format("delta").load(path_snapshot)

print("\n" + "="*60)
print("🛡️  GOLD LAYER: BUSINESS LOGIC & INTEGRITY GATE")
print("="*60)

# --- CHECK 1: Latest Snapshot Uniqueness ---
# Each coin should only have EXACTLY one record in the snapshot table
snap_count = df_snapshot.groupBy("coin_id").count().filter("count > 1").count()
if snap_count > 0:
    print(f"❌ INTEGRITY ERROR: Snapshot table has duplicate entries for {snap_count} coins!")
else:
    print(f"✅ Snapshot Integrity: Verified (Unique latest prices for {df_snapshot.count()} coins).")

# --- CHECK 2: Analytics Completeness ---
null_metrics = df_perf.filter(f.col("moving_avg_price").isNull() | f.col("price_volatility").isNull()).count()
if null_metrics > 0:
    print(f"❌ LOGIC ERROR: Found {null_metrics} records with missing Analytics.")
else:
    print("✅ Analytics Completeness: 100% (Moving Averages calculated).")

# --- CHECK 3: Range Reasonability ---
btc_anomalies = df_trends.filter((f.col("coin_id") == "bitcoin") & 
                                 ((f.col("daily_avg_price") < 20000) | (f.col("daily_avg_price") > 150000))).count()
if btc_anomalies > 0:
    print(f"❌ ANOMALY: {btc_anomalies} daily trends fall outside reasonable bounds!")
else:
    print("✅ Price Reasonability: Verified (No extreme outliers).")

# --- CHECK 4: Summary View (The Snapshot) ---
print("\n💎 CURRENT MARKET SNAPSHOT (Latest Prices):")
df_snapshot.select("coin_id", "price_usd", "event_timestamp").show()

print("="*60 + "\n")