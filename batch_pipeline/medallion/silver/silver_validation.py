from pyspark.sql import functions as f
from pyspark.sql.window import Window # Fixed Import
import datetime

# 1. Load the Table
df_check = spark.read.table("workspace.default.silver_crypto_prices")

print(" --- SILVER LAYER: FINAL SERVERLESS VALIDATION ---")

# --- 1. INTEGRITY CHECKS ---
duplicate_count = df_check.groupBy("coin_id", "event_timestamp").count().filter("count > 1").count()
print(f" Duplicate Records (expected 0): {duplicate_count}")

null_prices = df_check.filter(f.col("price_usd").isNull()).count()
print(f" Records with Null Prices (expected 0): {null_prices}")

# --- 2. TRANSFORMATION LOGIC CHECKS ---
# Serverless compatible list comprehension
flag_rows = df_check.select("price_change_flag").distinct().collect()
flag_values = [row["price_change_flag"] for row in flag_rows]
print(f" Logic Check: Detected Price Flags: {flag_values}")

# --- 3. STORAGE & PARTITIONING ---
dates_count = df_check.select("date").distinct().count()
print(f" Storage Check: Total unique partitions (days): {dates_count}")

# --- 4. ADVANCED DATA OBSERVABILITY (SLA & FRESHNESS) ---
latest_ts = df_check.select(f.max("event_timestamp")).collect()[0][0]
current_ts = datetime.datetime.now()
delay_mins = (current_ts - latest_ts.replace(tzinfo=None)).total_seconds() / 60

if delay_mins > 30:
    print(f" SLA ALERT: Data is STALE. Last update was {round(delay_mins, 2)} minutes ago.")
else:
    print(f" Freshness Check: OK ({round(delay_mins, 2)} mins old).")

# --- 5. VOLUME & ANOMALY DETECTION ---
print(" Volume Check (Rows per Coin):")
coin_counts = df_check.groupBy("coin_id").count().orderBy("coin_id").collect()
for row in coin_counts:
    print(f"   - {row['coin_id']}: {row['count']} rows")

# Outlier Check
outliers = df_check.filter((f.col("coin_id") == "bitcoin") & 
                           ((f.col("price_usd") < 10000) | (f.col("price_usd") > 250000))).count()
if outliers > 0:
    print(f" ANOMALY: Detected {outliers} impossible price points for Bitcoin!")
else:
    print(" Reasonability Check: Bitcoin price range is valid.")

# Price Jump Check (FIXED Window Reference)
# Note: Window is called directly here, NOT via f.Window
jump_window = Window.partitionBy("coin_id").orderBy("event_timestamp")

df_jump = df_check.withColumn("prev_price", f.lag("price_usd").over(jump_window)) \
                  .withColumn("pct_change", f.abs((f.col("price_usd") - f.col("prev_price")) / f.col("prev_price")))

major_jumps = df_jump.filter(f.col("pct_change") > 0.20).count()
if major_jumps > 0:
    print(f" QUALITY WARNING: Found {major_jumps} instances of >20% price jumps.")
else:
    print(" Stability Check: No suspicious price jumps detected.")

# Lineage Check
negative_delay = df_check.filter(f.col("ingestion_delay_seconds") < 0).count()
if negative_delay > 0:
    print(f" TIME SYNC ERROR: {negative_delay} records have timestamps from the future!")
else:
    print(" Lineage Check: Clock synchronization is valid.")

print("----------------------------------------------------------")

# Final Visual Confirmation
display(df_check.sort(f.col("event_timestamp").desc()).limit(15))
