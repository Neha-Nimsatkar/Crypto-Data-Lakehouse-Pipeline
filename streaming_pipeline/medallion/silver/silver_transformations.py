import os
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, MapType

# 1. ENV SETUP (Local Paths)
os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

spark = SparkSession.builder \
    .appName("Crypto_Silver_Streaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# 2. DEFINE JSON SCHEMA (Essential for Streaming)
# This matches your CoinGecko JSON structure
json_schema = MapType(StringType(), StructType([
    StructField("usd", DoubleType()),
    StructField("usd_market_cap", DoubleType()),
    StructField("usd_24h_vol", DoubleType()),
    StructField("last_updated_at", DoubleType())
]))

# 3. READ STREAM FROM BRONZE
bronze_path = "A:/Crypto-Data-lakehouse-pipeline/data/bronze/crypto_prices_delta"
df_stream = spark.readStream.format("delta").load(bronze_path)

# 4. TRANSFORMATION & QUALITY FILTERING
# Converting Binary value from Kafka/Bronze to Strings and Map
df_parsed = df_stream.select(
    f.from_json(f.col("value").cast("string"), json_schema).alias("data"),
    f.col("timestamp").alias("kafka_arrival_time")
)

# Flattening the Map (Replacing your 'Create Map' logic with a dynamic Explode)
df_exploded = df_parsed.select(
    f.explode(f.col("data")).alias("coin_id", "metrics"),
    "kafka_arrival_time"
)

# Cleaning, Casting, and Watermarking
# WATERMARK: Tells Spark to only keep 10 minutes of data in memory for deduplication
df_cleaned = df_exploded.select(
    f.lower(f.col("coin_id")).alias("coin_id"),
    f.col("metrics.usd").alias("price_usd"),
    f.col("metrics.usd_market_cap").alias("market_cap"),
    f.col("metrics.usd_24h_vol").alias("volume_24h"),
    f.to_timestamp(f.from_unixtime(f.col("metrics.last_updated_at"))).alias("event_timestamp"),
    f.col("kafka_arrival_time").alias("ingested_at")
).withWatermark("event_timestamp", "10 minutes") 

# 5. DEDUPLICATION (Streaming version)
# In streaming, we use dropDuplicates instead of row_number() window for better performance
df_deduped = df_cleaned.dropDuplicates(["coin_id", "event_timestamp"])

# 6. WRITE STREAM TO SILVER
silver_path = "A:/Crypto-Data-lakehouse-pipeline/data/silver/crypto_prices_clean"
checkpoint_silver = "A:/Crypto-Data-lakehouse-pipeline/data/checkpoint/silver"

query = df_deduped.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_silver) \
    .partitionBy("coin_id") \
    .start(silver_path)

print(f"🚀 Silver Stream Started! Cleaning data into {silver_path}")
query.awaitTermination()
