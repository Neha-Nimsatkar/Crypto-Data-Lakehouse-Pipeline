"""
File        : silver_transformations.py
Location    : batch_pipeline/medallion/silver/
Description : Transforms raw Bronze layer JSON data into a clean, structured
              Silver Delta table. Performs flattening, casting, deduplication,
              and enrichment of cryptocurrency price data.

Input       : s3://crypto-lakehouse-neha/bronze/*.json
Output      : s3://crypto-lakehouse-neha/silver/crypto_prices
              (Delta table: workspace.default.silver_crypto_prices)

Transformations Applied:
    1. Dynamic flattening of nested JSON coin structure
    2. Type casting (price, market cap, volume to double)
    3. Timestamp conversion and partitioning by date
    4. Ingestion delay calculation
    5. Price change flag (UP / DOWN / STABLE) using window functions
    6. Deduplication by coin and event timestamp
    7. Load timestamp added for lineage tracking

Dependencies:
    - pyspark
    - delta-spark
    - AWS S3 access configured

Warning:
    Requires active SparkSession. Run inside Databricks or
    an environment with PySpark and S3 access configured.
    Table drop statements at the top are optional — uncomment
    only when a full schema reset is needed.
"""


from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ── Configuration ─────────────────────────────────────────────────────────────
BRONZE_PATH   = "s3://crypto-lakehouse-neha/bronze/*.json"
SILVER_PATH   = "s3://crypto-lakehouse-neha/silver/crypto_prices"
CATALOG       = "workspace"
SCHEMA        = "default"
SILVER_TABLE  = f"{CATALOG}.{SCHEMA}.silver_crypto_prices"
COINS         = ["bitcoin", "ethereum", "solana"]


# ── Optional: Schema Reset (uncomment only when full reset is needed) ──────────
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.silver_crypto_prices")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.gold_latest_snapshot")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.gold_price_performance")
# spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.gold_daily_trends")


# ── Step 1: Read Raw Bronze Data ──────────────────────────────────────────────
print("─" * 60)
print("  SILVER TRANSFORMATION: START")
print("─" * 60)

df_raw = spark.read.option("multiLine", "true").json(BRONZE_PATH)
print(f"\n[STEP 1] Bronze data loaded | Records: {df_raw.count()}")


# ── Step 2: Flatten Nested JSON Structure ─────────────────────────────────────
df_with_meta = df_raw.select(
    F.col("ingestion_metadata.ingested_at").alias("ingested_at_str"),
    *[F.col(coin) for coin in COINS]
)


# Explode coin columns into rows (tidy data format)
df_exploded = df_with_meta.select(
    "ingested_at_str",
    F.explode(
        F.create_map(*[item for coin in COINS for item in (F.lit(coin), F.col(coin))])
    ).alias("coin_id", "data")
)
print(f"[STEP 2] JSON flattened | Exploded records: {df_exploded.count()}")


# ── Step 3: Clean and Cast Columns ───────────────────────────────────────────
df_cleaned = df_exploded.select(
    F.lower(F.col("coin_id")).alias("coin_id"),
    F.col("data.usd").cast("double").alias("price_usd"),
    F.col("data.usd_market_cap").cast("double").alias("market_cap"),
    F.col("data.usd_24h_vol").cast("double").alias("volume_24h"),
    F.col("data.last_updated_at").alias("api_last_updated_at"),
    F.to_timestamp(F.col("ingested_at_str")).alias("ingested_at"),
).filter(F.col("price_usd").isNotNull())
print(f"[STEP 3] Cleaned and cast | Valid records: {df_cleaned.count()}")


# ── Step 4: Timestamps and Partitioning Columns ───────────────────────────────
df_transformed = (
    df_cleaned
    .withColumn("event_timestamp", F.to_timestamp(F.from_unixtime(F.col("api_last_updated_at"))))
    .withColumn("date", F.to_date(F.col("event_timestamp")))
    .withColumn("hour", F.hour(F.col("event_timestamp")))
)
print("[STEP 4] Timestamps extracted | date and hour columns added")


# ── Step 5: Derived Metrics ───────────────────────────────────────────────────
df_metrics = df_transformed.withColumn(
    "ingestion_delay_seconds",
    F.unix_timestamp(F.col("ingested_at")) - F.col("api_last_updated_at")
)

price_window = Window.partitionBy("coin_id").orderBy("event_timestamp")

df_flags = (
    df_metrics
    .withColumn("prev_price", F.lag("price_usd").over(price_window))
    .withColumn(
        "price_change_flag",
        F.when(F.col("price_usd") > F.col("prev_price"), "UP")
         .when(F.col("price_usd") < F.col("prev_price"), "DOWN")
         .otherwise("STABLE")
    )
)
print("[STEP 5] Derived metrics added | ingestion_delay and price_change_flag computed")


# ── Step 6: Deduplication ─────────────────────────────────────────────────────
dedup_window = Window.partitionBy("coin_id", "event_timestamp").orderBy(F.col("ingested_at").desc())

df_deduped = (
    df_flags
    .withColumn("rn", F.row_number().over(dedup_window))
    .filter(F.col("rn") == 1)
    .drop("rn", "prev_price")
)
print(f"[STEP 6] Deduplicated | Final records: {df_deduped.count()}")


# ── Step 7: Add Load Timestamp ────────────────────────────────────────────────
df_final = df_deduped.withColumn("load_timestamp", F.current_timestamp())
print("[STEP 7] Load timestamp added")


# ── Write to Silver Delta Table ───────────────────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"\n[WRITE] Writing to Silver Delta table: {SILVER_TABLE}")
print(f"[WRITE] S3 path: {SILVER_PATH}")

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .option("path", SILVER_PATH)
    .saveAsTable(SILVER_TABLE)
)

print("\n" + "─" * 60)
print(f"  SUCCESS: Silver data written to {SILVER_PATH}")
print("─" * 60)