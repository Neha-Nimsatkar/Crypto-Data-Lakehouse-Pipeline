# COMMAND ----------
# File: notebooks/03_bronze_to_silver_pipeline.py
# Description: Real-Time Stream from Bronze to Silver (Fixed Parsing Syntax)
# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime, to_timestamp, to_date, hour, current_timestamp, lit
from pyspark.sql.types import DoubleType, LongType

print("─" * 60)
BITCOIN_PRICE_MIN = 10000
BITCOIN_PRICE_MAX = 250000

# ── STEP 1: STREAMING READ FROM BRONZE DELTA TABLE ────────────────────────────
streaming_bronze_df = spark.readStream.table("workspace.default.crypto_bronze_table")
print(" Streaming source from crypto_bronze_table initialized...")

# ── STEP 2: INLINE BRONZE VALIDATION ──────────────────────────────────────────
validated_bronze_df = streaming_bronze_df.filter(
    (col("symbol").isNotNull()) & 
    (col("price") > 0) &
    (~((col("symbol") == "bitcoin") & ((col("price") < BITCOIN_PRICE_MIN) | (col("price") > BITCOIN_PRICE_MAX))))
)

# ── STEP 3: CORE SILVER TRANSFORMATIONS & PARSING FIX ─────────────────────────
# Using explicit column operations to avoid the parse syntax slash (/) error
df_transformed = validated_bronze_df \
    .withColumn("coin_id", col("symbol")) \
    .withColumn("price_usd", col("price").cast(DoubleType())) \
    .withColumn("volume_24h", col("volume").cast(DoubleType())) \
    .withColumn("event_timestamp", to_timestamp(from_unixtime(col("timestamp").cast(LongType()) / 1000))) \
    .withColumn("date", to_date(col("event_timestamp"))) \
    .withColumn("hour", hour(col("event_timestamp"))) \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("ingestion_delay_seconds", col("ingested_at").cast(LongType()) - (col("timestamp").cast(LongType()) / 1000))

# ── STEP 4: REAL-TIME DEDUPLICATION STATE HANDLER ─────────────────────────────
df_deduped = df_transformed.dropDuplicates(["coin_id", "timestamp"])

# ── STEP 5: DRIFT & PRODUCTION LINEAGE CHECK ──────────────────────────────────
df_final = df_deduped.filter(col("ingestion_delay_seconds") >= 0) \
                     .withColumn("load_timestamp", current_timestamp())



# ── STEP 6: STREAMING SINK WITH SCHEMA EVOLUTION ACTIVATED ──────────────────
SILVER_CHECKPOINT = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/silver_stream_pipeline_v4/"

print(" Schema auto-merge capability activated. Appending records...")

# Clean logic block without inline comments breaking the backslashes
query_silver = df_final.writeStream \
    .format("delta") \
    .option("checkpointLocation", SILVER_CHECKPOINT) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .toTable("workspace.default.silver_crypto_prices")

    
# ── MAGIC LINE: Enforces the script to wait until the execution stream safely ends ──
query_silver.awaitTermination()

print(" Pipeline run completed successfully! Schema evolved and data loaded without syntax drops.")
