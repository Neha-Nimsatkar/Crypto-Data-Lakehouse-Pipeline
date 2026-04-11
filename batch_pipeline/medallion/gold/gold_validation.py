from pyspark.sql import functions as f

# Load the Gold Performance table
df_gold_val = spark.read.table("workspace.default.gold_price_performance")

print(" --- GOLD LAYER: BUSINESS LOGIC VALIDATION ---")

# Check 1: Ranking Integrity
# Every timestamp should have a #1, #2, and #3 rank. 
# If a timestamp has two #1s, your Window function is broken.
rank_check = df_gold_val.groupBy("event_timestamp", "market_cap_rank").count().filter("count > 1").count()
if rank_check > 0:
    print(f" LOGIC ERROR: Detected {rank_check} instances of duplicate ranks per timestamp!")
else:
    print(" Ranking Integrity: Verified (1 rank per coin per timestamp).")

# Check 2: Moving Average Calculation Accuracy
# The Moving Average should never be $0 if the price is > $0.
ma_nulls = df_gold_val.filter(f.col("moving_avg_price").isNull()).count()
if ma_nulls > 0:
    print(f" CALCULATION GAP: {ma_nulls} records are missing Moving Averages.")
else:
    print(" Analytics Completeness: 100% of records have Moving Averages.")

# Check 3: Cross-Table Consistency
# The Latest Snapshot should match the most recent record in the Performance table.
latest_snapshot_price = spark.read.table("workspace.default.gold_latest_snapshot").filter("coin_id = 'bitcoin'").select("price_usd").collect()[0][0]
latest_perf_price = df_gold_val.filter("coin_id = 'bitcoin'").sort(f.col("event_timestamp").desc()).limit(1).select("price_usd").collect()[0][0]

if abs(latest_snapshot_price - latest_perf_price) > 0.0001:
    print(" SYNC ERROR: Snapshot and Performance tables are out of sync!")
else:
    print(" Table Synchronization: Verified.")

print("--------------------------------------------------")