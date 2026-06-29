
# validates silver delta table before promoting to gold
# checks duplicates, nulls, price flags, freshness, volume, anomalies, and lineage
# runs on Databricks — needs active SparkSession and Unity Catalog access
# display() is Databricks-native, won't work outside Databricks


from pyspark.sql import functions as F
from pyspark.sql.window import Window


CATALOG = "workspace"
SCHEMA = "default"
TABLE_NAME = f"{CATALOG}.{SCHEMA}.silver_crypto_prices"

SLA_THRESHOLD_MIN = 30
BITCOIN_PRICE_MIN = 10_000
BITCOIN_PRICE_MAX = 250_000
PRICE_JUMP_PCT = 0.20


df_check = spark.read.table(TABLE_NAME)


print("silver table validations begin..")



# 1: Integrity 
print("\ncheck 1 - Integrity")

duplicate_count = (
    df_check.groupBy("coin_id", "event_timestamp")
    .count()
    .filter("count > 1")
    .count()
)
null_prices = df_check.filter(F.col("price_usd").isNull()).count()

print(f"{'pass' if duplicate_count == 0 else 'fail'} — duplicates: {duplicate_count}")
print(f" {'Pass' if null_prices == 0 else 'Fail'} : NULL price records: {null_prices}")


# 2: Transformation Logic 
print("\ncheck 2 -Transformation Logic")


# check what flag values exist
flag_rows = df_check.select("price_change_flag").distinct().collect()
flag_values = [row["price_change_flag"] for row in flag_rows]
print(f"price change flags found: {flag_values}")

# 3: Storage & Partitioning 
print("\ncheck 3 - Storage & Partitioning")

dates_count = df_check.select("date").distinct().count()
print(f"Unique date partitions : {dates_count}")


# 4: Data Freshness
print("\ncheck 4 - Data Freshness")

try:
    
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
        print(" error - no records found to process freshness checks.")
    elif delay_mins > SLA_THRESHOLD_MIN:
        print(f"alert — data is stale, last update {round(delay_mins, 2)} mins ago")
    else:
        print(f"pass — data is fresh, {round(delay_mins, 2)} mins old")
except Exception as e:
    print(f" error - could not calculate data freshness metrics: {e}")


# 5: Volume per Coin 
print("\ncheck 5 - Volume")

coin_counts = (
    df_check.groupBy("coin_id")
    .count()
    .orderBy("coin_id")
    .collect()
)
print(f" total unique assets tracked: {len(coin_counts)}")
for row in coin_counts:
    print(f" {row['coin_id']:<15} — {row['count']} rows")


# 6: Anomaly Detection 
print("\ncheck 6 - Anomaly Detection")

outliers = df_check.filter(
    (F.col("coin_id") == "bitcoin") &
    ((F.col("price_usd") < BITCOIN_PRICE_MIN) | (F.col("price_usd") > BITCOIN_PRICE_MAX))
).count()

if outliers > 0:
    print(f"fail — {outliers} impossible bitcoin prices found")
else:
    print(f"Pass - Bitcoin price range valid ({BITCOIN_PRICE_MIN:,} – {BITCOIN_PRICE_MAX:,})")


# 7: Price Stability 
print("\ncheck 7 - Price Stability")

jump_window = Window.partitionBy("coin_id").orderBy("event_timestamp")


df_jump = (
    df_check
    .withColumn("prev_price", F.lag("price_usd").over(jump_window))
    .withColumn("pct_change", F.abs(
        (F.col("price_usd") - F.col("prev_price")) / F.coalesce(F.col("prev_price"), F.col("price_usd"))
    ))
)

major_jumps = df_jump.filter(F.col("pct_change") > PRICE_JUMP_PCT).count()

if major_jumps > 0:
    print(f" error - {major_jumps} instance(s) of >{int(PRICE_JUMP_PCT * 100)}% price jump detected")
else:
    print(f"pass — no price jumps over {int(PRICE_JUMP_PCT * 100)}%")


# 8: Lineage & Clock Sync
print("\ncheck 8 - Lineage")

negative_delay = df_check.filter(F.col("ingestion_delay_seconds") < 0).count()

if negative_delay > 0:
    print(f"Fail - {negative_delay} record(s) have future timestamps (clock sync issue)")
else:
    print(f"pass — clock sync looks fine")




print(" silver validation completed")

display(df_check.sort(F.col("event_timestamp").desc()).limit(15))