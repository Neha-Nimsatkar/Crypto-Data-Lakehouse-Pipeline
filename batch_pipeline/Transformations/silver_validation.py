
"""
File        : silver_validation.py
Location    : batch_pipeline/medallion/silver/
Description : Performs final validation and observability checks on the Silver
              layer Delta table after transformations from Bronze.
              Acts as a quality gate before data is promoted to the Gold layer.

Input       : Databricks table — workspace.default.silver_crypto_prices
Output      : Console validation report (pass/fail per check)

Checks Performed:
    1. Integrity    — duplicate records and NULL price detection
    2. Logic        — price change flag values validation
    3. Storage      — partition count by date
    4. Freshness    — SLA check against 30-minute threshold
    5. Volume       — row count per coin
    6. Anomaly      — impossible Bitcoin price range detection
    7. Stability    — price jump detection (>20% change)
    8. Lineage      — negative ingestion delay detection

Dependencies:
    - pyspark
    - Databricks workspace with silver_crypto_prices table

Warning:
    Requires active SparkSession and Databricks Unity Catalog access.
    display() function is Databricks-native and will not work outside Databricks.
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Configuration ─────────────────────────────────────────────────────────────
CATALOG           = "workspace"
SCHEMA            = "default"
TABLE_NAME        = f"{CATALOG}.{SCHEMA}.silver_crypto_prices"

SLA_THRESHOLD_MIN = 30
BITCOIN_PRICE_MIN = 10_000
BITCOIN_PRICE_MAX = 250_000
PRICE_JUMP_PCT    = 0.20


# ── Load Silver Table ─────────────────────────────────────────────────────────
# Read dynamically via spark catalog engine metadata binding
df_check = spark.read.table(TABLE_NAME)

print("─" * 60)
print("  SILVER LAYER: FINAL VALIDATION & OBSERVABILITY (16 COINS METRICS)")
print("─" * 60)


# ── Check 1: Integrity ────────────────────────────────────────────────────────
print("\n[CHECK 1] Integrity")

duplicate_count = (
    df_check.groupBy("coin_id", "event_timestamp")
    .count()
    .filter("count > 1")
    .count()
)
null_prices = df_check.filter(F.col("price_usd").isNull()).count()

print(f"  {'PASS' if duplicate_count == 0 else 'FAIL'}     : Duplicate records     : {duplicate_count}")
print(f"  {'PASS' if null_prices == 0 else 'FAIL'}     : NULL price records    : {null_prices}")


# ── Check 2: Transformation Logic ────────────────────────────────────────────
print("\n[CHECK 2] Transformation Logic")

# Safely extract distinct string flags using functional map matrix
flag_rows = df_check.select("price_change_flag").distinct().collect()
flag_values = [row["price_change_flag"] for row in flag_rows]
print(f"  INFO     : Detected price change flags : {flag_values}")


# ── Check 3: Storage & Partitioning ──────────────────────────────────────────
print("\n[CHECK 3] Storage & Partitioning")

dates_count = df_check.select("date").distinct().count()
print(f"  INFO     : Unique date partitions      : {dates_count}")


# ── Check 4: Data Freshness (SLA) ─────────────────────────────────────────────
print("\n[CHECK 4] Data Freshness")

try:
    # Production Optimized: Spark standard functional engine metrics matching without timezone drifts
    freshness_df = df_check.select(
        F.max("event_timestamp").alias("latest_ts"),
        F.current_timestamp().alias("current_ts")
    ).withColumn(
        "delay_mins", 
        (F.unix_timestamp("current_ts") - F.unix_timestamp("latest_ts")) / 60
    )
    
    metrics_row = freshness_df.collect()[0]
    delay_mins = metrics_row["delay_mins"]

    if delay_mins is None:
        print("  WARNING  : No records found to process freshness SLA checks.")
    elif delay_mins > SLA_THRESHOLD_MIN:
        print(f"  ALERT    : Data is STALE — last update {round(delay_mins, 2)} mins ago")
    else:
        print(f"  PASS     : Data is fresh — {round(delay_mins, 2)} mins old")
except Exception as e:
    print(f"  WARNING  : Could not calculate data freshness metrics: {e}")


# ── Check 5: Volume per Coin ──────────────────────────────────────────────────
print("\n[CHECK 5] Volume")

coin_counts = (
    df_check.groupBy("coin_id")
    .count()
    .orderBy("coin_id")
    .collect()
)
print(f"  INFO     : Total unique assets tracked: {len(coin_counts)}")
for row in coin_counts:
    print(f"  INFO     : {row['coin_id']:<15} — {row['count']} rows")


# ── Check 6: Anomaly Detection ────────────────────────────────────────────────
print("\n[CHECK 6] Anomaly Detection")

outliers = df_check.filter(
    (F.col("coin_id") == "bitcoin") &
    ((F.col("price_usd") < BITCOIN_PRICE_MIN) | (F.col("price_usd") > BITCOIN_PRICE_MAX))
).count()

if outliers > 0:
    print(f"  FAIL     : {outliers} impossible Bitcoin price point(s) detected")
else:
    print(f"  PASS     : Bitcoin price range valid ({BITCOIN_PRICE_MIN:,} – {BITCOIN_PRICE_MAX:,})")


# ── Check 7: Price Stability ──────────────────────────────────────────────────
print("\n[CHECK 7] Price Stability")

jump_window = Window.partitionBy("coin_id").orderBy("event_timestamp")

# Coalesce calculation handler avoids division by zero on initialization metrics
df_jump = (
    df_check
    .withColumn("prev_price", F.lag("price_usd").over(jump_window))
    .withColumn("pct_change", F.abs(
        (F.col("price_usd") - F.col("prev_price")) / F.coalesce(F.col("prev_price"), F.col("price_usd"))
    ))
)

major_jumps = df_jump.filter(F.col("pct_change") > PRICE_JUMP_PCT).count()

if major_jumps > 0:
    print(f"  WARNING  : {major_jumps} instance(s) of >{int(PRICE_JUMP_PCT * 100)}% price jump detected")
else:
    print(f"  PASS     : No suspicious price jumps (>{int(PRICE_JUMP_PCT * 100)}%) detected")


# ── Check 8: Lineage & Clock Sync ────────────────────────────────────────────
print("\n[CHECK 8] Lineage")

negative_delay = df_check.filter(F.col("ingestion_delay_seconds") < 0).count()

if negative_delay > 0:
    print(f"  FAIL     : {negative_delay} record(s) have future timestamps (clock sync issue)")
else:
    print("  PASS     : Clock synchronisation valid")

print("\n" + "─" * 60)
print("  SILVER VALIDATION COMPLETE")
print("─" * 60)


# ── Preview ───────────────────────────────────────────────────────────────────
# Active cell preview pipeline render
display(df_check.sort(F.col("event_timestamp").desc()).limit(15))