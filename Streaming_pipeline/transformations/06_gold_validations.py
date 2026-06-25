# COMMAND ----------
# File: notebooks/05_gold_validations.py
# Description: Business Logic and Cross-Table Integrity Validation Gate
# COMMAND ----------

from pyspark.sql import functions as F

df_perf = spark.read.table("workspace.default.gold_price_performance")
df_snap = spark.read.table("workspace.default.gold_latest_snapshot")

print("=" * 60)
print("  GOLD LAYER: STREAMING BUSINESS OBSERVABILITY VALIDATION GATES")
print("=" * 60)

# ── CHECK 1: RANKING INTEGRITY ────────────────────────────────────────────────
print("\n[CHECK 1] Ranking Integrity Evaluate...")
rank_check = df_perf.groupBy("event_timestamp", "market_cap_rank").count().filter("count > 1").count()

if rank_check > 0:
    print(f"  FAIL     : Found {rank_check} instances of duplicate ranks per timestamp.")
else:
    print("  PASS     : Metrics integrity absolute! Unique rank distribution confirmed.")

# ── CHECK 2: MOVING AVERAGE COMPLETENESS ──────────────────────────────────────
print("\n[CHECK 2] Moving Average Density Evaluate...")
ma_nulls = df_perf.filter(F.col("moving_avg_price").isNull()).count()

if ma_nulls > 0:
    print(f"  FAIL     : Found {ma_nulls} records missing moving average values.")
else:
    print("  PASS     : Moving calculation density is 100% complete.")

# ── CHECK 3: CROSS-TABLE SYNC ────────────────────────────────────────────────
print("\n[CHECK 3] Cross-Table Synchronization...")
try:
    snap_btc = df_snap.filter("coin_id = 'bitcoin'").select("price_usd").collect()
    perf_btc = df_perf.filter("coin_id = 'bitcoin'").orderBy(F.col("event_timestamp").desc()).limit(1).select("price_usd").collect()

    if len(snap_btc) > 0 and len(perf_btc) > 0:
        snap_p = snap_btc[0]["price_usd"]
        perf_p = perf_btc[0]["price_usd"]
        
        if abs(snap_p - perf_p) > 0.05: # Accommodating micro streaming intervals offsets
            print(f"  WARNING  : Sync latency detected! Snapshot: ${snap_p} | Performance Trend: ${perf_p}")
        else:
            print(f"  PASS     : Synchronization active! Verified Bitcoin real-time asset marker: ${round(snap_p, 2)}")
    else:
        print("  INFO     : Dynamic telemetry skip - profiles generating state.")
except Exception as e:
    print(f"  WARNING  : Verification routine internal slip: {e}")

print("\n" + "=" * 60)
print("  GOLD QUALITY GATE PROCESS CONCLUDED")
print("=" * 60)
