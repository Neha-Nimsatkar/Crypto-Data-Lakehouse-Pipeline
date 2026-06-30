# notebook
# sanity checks on the gold tables - ranking consistency, moving avg coverage, snapshot vs perf table sync


from pyspark.sql import functions as F

df_perf = spark.read.table("workspace.default.gold_price_performance")
df_snap = spark.read.table("workspace.default.gold_latest_snapshot")

print("running gold layer checks...")



# 1: RANKING INTEGRITY
print("\ncheck 1 - duplicate ranks per timestamp...")
rank_check = df_perf.groupBy("event_timestamp", "market_cap_rank").count().filter("count > 1").count()

if rank_check > 0:
    print(f"fail - found {rank_check} duplicate ranks per timestamp")
else:
    print("pass - ranks look clean")



# 2: MOVING AVERAGE COMPLETENESS
print("\ncheck 2 - moving avg nulls...")
ma_nulls = df_perf.filter(F.col("moving_avg_price").isNull()).count()

if ma_nulls > 0:
    print(f"fail - found {ma_nulls} records missing moving average values")
else:
    print("pass - moving avg is fully populated")



# 3: CROSS-TABLE SYNC
print("\ncheck 3 - snapshot vs performance table sync...")
try:
    snap_btc = df_snap.filter("coin_id = 'bitcoin'").select("price_usd").collect()
    perf_btc = df_perf.filter("coin_id = 'bitcoin'").orderBy(F.col("event_timestamp").desc()).limit(1).select("price_usd").collect()

    if len(snap_btc) > 0 and len(perf_btc) > 0:
        snap_p = snap_btc[0]["price_usd"]
        perf_p = perf_btc[0]["price_usd"]
        
        if abs(snap_p - perf_p) > 0.05: # Accommodating micro streaming intervals offsets
            print(f"sync gap - snapshot: ${snap_p} | performance: ${perf_p}")
        else:
            print(f"pass - btc price in sync: ${round(snap_p, 2)}")
    else:
        print("skipping - one of the tables has no bitcoin rows yet")

except Exception as e:
    print(f"check 3 errored out: {e}")



print("gold layer checks done")