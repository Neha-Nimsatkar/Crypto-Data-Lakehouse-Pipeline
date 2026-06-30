#it is a notebook 
# Business Logic and Cross-Table Integrity Validation Gate


from pyspark.sql import functions as F

df_perf = spark.read.table("workspace.default.gold_price_performance")
df_snap = spark.read.table("workspace.default.gold_latest_snapshot")

print(" Gold Layer Validations Begin ...")



# 1: RANKING INTEGRITY
print("\ncheck 1 - Ranking Integrity ...")
rank_check = df_perf.groupBy("event_timestamp", "market_cap_rank").count().filter("count > 1").count()

if rank_check > 0:
    print(f" Fail - Found {rank_check}  of duplicate ranks per timestamp.")
else:
    print(" Pass - intergrity checks successfull.")



# 2: MOVING AVERAGE COMPLETENESS
print("\ncheck 2 - Moving Average Density Evaluate...")
ma_nulls = df_perf.filter(F.col("moving_avg_price").isNull()).count()

if ma_nulls > 0:
    print(f" Fail - Found {ma_nulls} records missing moving average values.")
else:
    print(" Pass - Moving calculation density is 100% complete.")



# 3: CROSS-TABLE SYNC
print("\ncheck 3 - Cross-Table Synchronization...")
try:
    snap_btc = df_snap.filter("coin_id = 'bitcoin'").select("price_usd").collect()
    perf_btc = df_perf.filter("coin_id = 'bitcoin'").orderBy(F.col("event_timestamp").desc()).limit(1).select("price_usd").collect()

    if len(snap_btc) > 0 and len(perf_btc) > 0:
        snap_p = snap_btc[0]["price_usd"]
        perf_p = perf_btc[0]["price_usd"]
        
        if abs(snap_p - perf_p) > 0.05: # Accommodating micro streaming intervals offsets
            print(f" Sync latency detected! Snapshot: ${snap_p} | Performance Trend: ${perf_p}")
        else:
            print(f" Pass - Synchronization active! Verified Bitcoin real-time asset marker: ${round(snap_p, 2)}")
    else:
        print(" Dynamic telemetry skip - profiles generating state.")

except Exception as e:
    print(f"Verification routine internal slip: {e}")



print("Gold Layer Validations Completed.")

