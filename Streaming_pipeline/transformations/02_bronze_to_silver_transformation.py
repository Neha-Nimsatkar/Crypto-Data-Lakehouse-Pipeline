# bronze to silver: cleans up raw ticks, adds validation + dedup, casts types properly
# second hop in the medallion setup
# writes out to the silver volume as a delta table


from pyspark.sql.functions import col, from_unixtime, to_timestamp, to_date, hour, current_timestamp, lit
from pyspark.sql.types import DoubleType, LongType

BITCOIN_PRICE_MIN = 10000
BITCOIN_PRICE_MAX = 250000

streaming_bronze_df = spark.readStream.table("workspace.default.crypto_bronze_table")
print("kicking off the transforms...")


validated_bronze_df = streaming_bronze_df.filter(
    (col("symbol").isNotNull()) & 
    (col("price") > 0) &
    (~((col("symbol") == "bitcoin") & ((col("price") < BITCOIN_PRICE_MIN) | (col("price") > BITCOIN_PRICE_MAX))))
)


df_transformed = validated_bronze_df \
    .withColumn("coin_id", col("symbol")) \
    .withColumn("price_usd", col("price").cast(DoubleType())) \
    .withColumn("volume_24h", col("volume").cast(DoubleType())) \
    .withColumn("event_timestamp", to_timestamp(from_unixtime(col("timestamp").cast(LongType()) / 1000))) \
    .withColumn("date", to_date(col("event_timestamp"))) \
    .withColumn("hour", hour(col("event_timestamp"))) \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("ingestion_delay_seconds", col("ingested_at").cast(LongType()) - (col("timestamp").cast(LongType()) / 1000))


df_deduped = df_transformed.dropDuplicates(["coin_id", "timestamp"])


df_final = df_deduped.filter(col("ingestion_delay_seconds") >= 0) \
                     .withColumn("load_timestamp", current_timestamp())



SILVER_CHECKPOINT = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/silver_stream_pipeline_v4/"


query_silver = df_final.writeStream \
    .format("delta") \
    .option("checkpointLocation", SILVER_CHECKPOINT) \
    .option("mergeSchema", "true") \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .toTable("workspace.default.silver_crypto_prices")

    

query_silver.awaitTermination()

print("silver layer write finished")