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

path_perf = "A:/Crypto-Data-lakehouse-pipeline/data/gold/price_performance"
path_trends = "A:/Crypto-Data-lakehouse-pipeline/data/gold/daily_trends"

df_perf = spark.read.format("delta").load(path_perf)
df_trends = spark.read.format("delta").load(path_trends)

# ... (Keep the rest of your print statements and check logic as it was) ...
print("✅ Gold Quality Checks Complete.")

print("\n" + "="*60)
print("🛡️  GOLD LAYER: BUSINESS LOGIC & INTEGRITY GATE")
print("="*60)

# --- CHECK 1: Calculation Completeness ---
# Ensure Moving Average and Volatility aren't just empty nulls
null_metrics = df_perf.filter(f.col("moving_avg_price").isNull() | f.col("price_volatility").isNull()).count()

if null_metrics > 0:
    print(f"❌ LOGIC ERROR: Found {null_metrics} records with missing Analytics (MA/Volatility).")
else:
    print("✅ Analytics Completeness: 100% (Moving Averages calculated).")

# --- CHECK 2: Range Reasonability (Business Constraint) ---
# Example: Bitcoin should not be $0 or $1,000,000 in our Gold aggregates
btc_anomalies = df_trends.filter((f.col("coin_id") == "bitcoin") & 
                                 ((f.col("daily_avg_price") < 30000) | (f.col("daily_avg_price") > 150000))).count()

if btc_anomalies > 0:
    print(f"❌ ANOMALY: {btc_anomalies} daily trends fall outside reasonable price bounds!")
else:
    print("✅ Price Reasonability: Verified (No extreme outliers in Gold).")

# --- CHECK 3: Window Integrity ---
# Ensure we don't have multiple trend records for the exact same coin and time window
window_dupes = df_trends.groupBy("coin_id", "date").count().filter("count > 1").count()

if window_dupes > 0:
    print(f"❌ INTEGRITY ERROR: Detected {window_dupes} duplicate time windows in Trends!")
else:
    print("✅ Window Integrity: Verified (1 record per coin per time window).")

# --- CHECK 4: Summary View ---
print("\n📈 Current Performance Summary (Latest per Coin):")
df_perf.orderBy(f.col("window.end").desc()).show(5, truncate=False)

print("="*60 + "\n")