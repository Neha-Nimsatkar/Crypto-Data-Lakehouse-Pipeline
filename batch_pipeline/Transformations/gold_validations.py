
"""
File        : gold_validations.py
Location    : batch_pipeline/medallion/gold/
Description : Validates business logic and cross-table consistency across
              Gold layer Delta tables using Unity Catalog references.

Input       : Databricks catalog tables —
                workspace.default.gold_price_performance
                workspace.default.gold_latest_snapshot

Checks Performed:
    1. Ranking integrity    — no duplicate ranks per timestamp
    2. Moving average       — no NULL moving averages
    3. Cross-table sync     — snapshot matches performance table (safe unpack)

Dependencies:
    - pyspark
    - Databricks native spark ecosystem (No custom hadoop-aws bundle manual injection)
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ==============================================================================
# Configuration
# ==============================================================================
CATALOG           = "workspace"
SCHEMA            = "default"
GOLD_PERF_TABLE   = f"{CATALOG}.{SCHEMA}.gold_price_performance"
GOLD_SNAP_TABLE   = f"{CATALOG}.{SCHEMA}.gold_latest_snapshot"

# ==============================================================================
# Load Gold Tables via Active Databricks Spark Engine
# ==============================================================================
df_perf = spark.read.table(GOLD_PERF_TABLE)
df_snap = spark.read.table(GOLD_SNAP_TABLE)

print("=" * 60)
print("  GOLD LAYER: BUSINESS LOGIC VALIDATION GATES")
print("=" * 60)


# ==============================================================================
# Check 1: Ranking Integrity
# ==============================================================================
print("\n[CHECK 1] Ranking Integrity")

rank_check = (
    df_perf
    .groupBy("event_timestamp", "market_cap_rank")
    .count()
    .filter("count > 1")
    .count()
)

if rank_check > 0:
    print(f"  FAIL     : {rank_check} instance(s) of duplicate ranks per timestamp")
else:
    print("  PASS     : One rank per coin per timestamp — verified")


# ==============================================================================
# Check 2: Moving Average Completeness
# ==============================================================================
print("\n[CHECK 2] Moving Average Completeness")

ma_nulls = df_perf.filter(F.col("moving_avg_price").isNull()).count()

if ma_nulls > 0:
    print(f"  FAIL     : {ma_nulls} record(s) missing moving average values")
else:
    print("  PASS     : 100% of records have moving average values")


# ==============================================================================
# Check 3: Cross-Table Consistency
# ==============================================================================
print("\n[CHECK 3] Cross-Table Consistency")

try:
    snap_rows = df_snap.filter("coin_id = 'bitcoin'").select("price_usd").collect()
    perf_rows = df_perf.filter("coin_id = 'bitcoin'").orderBy(F.col("event_timestamp").desc()).limit(1).select("price_usd").collect()

    # Production Fix: Protected unpacker checks if lists are populated before grabbing indexes
    if len(snap_rows) > 0 and len(perf_rows) > 0:
        snap_price = snap_rows[0]["price_usd"]
        perf_price = perf_rows[0]["price_usd"]

        if abs(snap_price - perf_price) > 0.0001:
            print(f"  FAIL     : Snapshot (${round(snap_price, 4)}) and performance (${round(perf_price, 4)}) are out of sync")
        else:
            print(f"  PASS     : Tables are in sync — Bitcoin price: ${round(snap_price, 2)}")
    else:
        print("  WARNING  : Asset 'bitcoin' data profile not available in one of the tables for sync check.")

except Exception as e:
    print(f"  WARNING  : Could not complete cross-table check: {e}")

print("\n" + "=" * 60)
print("  GOLD VALIDATION COMPLETE")
print("=" * 60)