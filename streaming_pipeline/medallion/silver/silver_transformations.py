import os
from pyspark.sql import SparkSession, functions as f
from delta import configure_spark_with_delta_pip


# ✅ KEY FIX: Force JAR resolution BEFORE SparkSession initializes
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.200 pyspark-shell"
)

AWS_ACCESS_KEY = "AKIAWYKG6KACJLEYZI65"
AWS_SECRET_KEY = "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"

builder = SparkSession.builder \
    .appName("Crypto_Silver_Streaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.path.style.access", "false") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "2") \
    .master("local[*]")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. PATHS — Bronze local, Silver + Checkpoint on S3
base_path    = "A:/Crypto-Data-lakehouse-pipeline"
bronze_path  = f"{base_path}/data/bronze/crypto_prices_delta"

s3_bucket       = "s3a://crypto-lakehouse-neha"
silver_path     = f"{s3_bucket}/silver/crypto_prices_clean"
checkpoint_silver = f"{s3_bucket}/checkpoints/silver"

# 3. READ STREAM from local Bronze
df_stream = spark.readStream.format("delta").load(bronze_path)

# 4. TRANSFORMATION
coins = ["bitcoin", "ethereum", "solana"]
current_unix_time = f.unix_timestamp()
df_with_time = df_stream.withColumn("fallback_time", current_unix_time)

stack_parts = []
for c in coins:
    price   = f"CAST({c}.usd AS DOUBLE)"
    m_cap   = f"COALESCE(CAST({c}.usd_market_cap AS DOUBLE), 0.0)"
    vol     = f"COALESCE(CAST({c}.usd_24h_vol AS DOUBLE), 0.0)"
    updated = f"COALESCE(CAST({c}.last_updated_at AS LONG), fallback_time)"
    stack_parts.append(f"'{c}', {price}, {m_cap}, {vol}, {updated}")

stack_str = ", ".join(stack_parts)
df_normalized = df_with_time.select(
    f.expr(
        f"stack({len(coins)}, {stack_str}) as (coin_id, price_usd, market_cap, volume_24h, updated_at)"
    ),
    f.col("ingestion_metadata.ingested_at").alias("ingested_at_str")
)

df_cleaned = df_normalized.select(
    "coin_id", "price_usd", "market_cap", "volume_24h",
    f.to_timestamp(f.from_unixtime(f.col("updated_at"))).alias("event_timestamp"),
    f.to_timestamp(f.col("ingested_at_str")).alias("ingested_at")
).filter(f.col("price_usd").isNotNull()) \
 .withWatermark("event_timestamp", "10 minutes")

# 5. WRITE TO S3 SILVER
query = df_cleaned.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_silver) \
    .start(silver_path)

print(f"🚀 Silver Stream Started! Writing to {silver_path}")
query.awaitTermination()