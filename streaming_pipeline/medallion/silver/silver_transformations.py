import os
from pyspark.sql import SparkSession, functions as f
from delta import configure_spark_with_delta_pip

# 1. ENV SETUP
os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

# Create temp folder for Spark to avoid ShutdownHook errors
temp_path = "A:/Crypto-Data-lakehouse-pipeline/data/spark_temp"
if not os.path.exists(temp_path):
    os.makedirs(temp_path)

builder = SparkSession.builder \
    .appName("Crypto_Silver_Streaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.warehouse.dir", "A:/Crypto-Data-lakehouse-pipeline/spark-warehouse") \
    .config("spark.local.dir", temp_path) \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 2. READ STREAM FROM BRONZE
bronze_path = "A:/Crypto-Data-lakehouse-pipeline/data/bronze/crypto_prices_delta"
df_stream = spark.readStream.format("delta").load(bronze_path)

# 3. TRANSFORMATION (Fixed for your specific Bronze schema)
# Your Bronze has columns: bitcoin, ethereum, solana, ingestion_metadata
# We combine them into a clean, normalized format
coins = ["bitcoin", "ethereum", "solana"]

# We use stack to turn columns into rows (Normalization)
stack_str = ", ".join([f"'{c}', {c}.usd, {c}.usd_market_cap, {c}.usd_24h_vol, {c}.last_updated_at" for c in coins])

df_normalized = df_stream.select(
    f.expr(f"stack({len(coins)}, {stack_str}) as (coin_id, price_usd, market_cap, volume_24h, updated_at)"),
    f.col("ingestion_metadata.ingested_at").alias("ingested_at")
)

# 4. CLEANING & WATERMARKING
df_cleaned = df_normalized.select(
    "coin_id",
    "price_usd",
    "market_cap",
    "volume_24h",
    f.to_timestamp(f.from_unixtime(f.col("updated_at"))).alias("event_timestamp"),
    "ingested_at"
).withWatermark("event_timestamp", "10 minutes")

# 5. DEDUPLICATION
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