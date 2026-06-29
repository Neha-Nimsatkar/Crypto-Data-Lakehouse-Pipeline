# validates gold layer delta tables — ranking, moving averages, and cross-table sync
# runs on Databricks with Unity Catalog


from pyspark.sql import functions as F
from pyspark.sql.window import Window


CATALOG = "workspace"
SCHEMA = "default"
GOLD_PERF_TABLE = f"{CATALOG}.{SCHEMA}.gold_price_performance"
GOLD_SNAP_TABLE = f"{CATALOG}.{SCHEMA}.gold_latest_snapshot"


df_perf = spark.read.table(GOLD_PERF_TABLE)
df_snap = spark.read.table(GOLD_SNAP_TABLE)

print("running gold validation checks")


# 1: Ranking Integrity

print("\n Check 1 - Ranking Integrity")

rank_check = (
    df_perf
    .groupBy("event_timestamp", "market_cap_rank")
    .count()
    .filter("count > 1")
    .count()
)

if rank_check > 0:
    print(f"  Fail - {rank_check} instance(s) of duplicate ranks per timestamp")
else:
    print("  Pass - One rank per coin per timestamp — verified")



# 2: Moving Average Completeness

print("\n Check 2 - Moving Average Completeness")

ma_nulls = df_perf.filter(F.col("moving_avg_price").isNull()).count()

if ma_nulls > 0:
    print(f"  Fail - {ma_nulls} record(s) missing moving average values")
else:
    print("pass — no null moving averages")



# 3: Cross-Table Consistency

print("\n Check 3 - Cross-Table Consistency")

try:
    snap_rows = df_snap.filter("coin_id = 'bitcoin'").select("price_usd").collect()
    perf_rows = df_perf.filter("coin_id = 'bitcoin'").orderBy(F.col("event_timestamp").desc()).limit(1).select("price_usd").collect()

    
    if len(snap_rows) > 0 and len(perf_rows) > 0:
        snap_price = snap_rows[0]["price_usd"]
        perf_price = perf_rows[0]["price_usd"]

        if abs(snap_price - perf_price) > 0.0001:
            print(f" Fail - Snapshot (${round(snap_price, 4)}) and performance (${round(perf_price, 4)}) are out of sync")
        else:
            print(f"Pass - Tables are in sync — Bitcoin price: ${round(snap_price, 2)}")
    else:
        print("error — bitcoin not found in one of the tables, skipping sync check")

except Exception as e:
    print(f"error - Could not complete cross-table check: {e}")

    


print("gold validations completed")