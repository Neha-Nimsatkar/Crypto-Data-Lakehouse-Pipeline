import os
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

# 1. ENV SETUP
os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

# ✅ KEY FIX: Force JAR resolution BEFORE SparkSession initializes
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.200 pyspark-shell"
)

AWS_ACCESS_KEY = "AKIAWYKG6KACJLEYZI65"
AWS_SECRET_KEY = "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"

builder = SparkSession.builder \
    .appName("Crypto_Gold_Streaming") \
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

# 2. PATHS — Silver on S3, Gold on S3
s3_bucket        = "s3a://crypto-lakehouse-neha"
silver_path      = f"{s3_bucket}/silver/crypto_prices_clean"

gold_path_trends = f"{s3_bucket}/gold/daily_trends"
gold_path_perf   = f"{s3_bucket}/gold/price_performance"
gold_path_snap   = f"{s3_bucket}/gold/latest_snapshot"
checkpoint_base  = f"{s3_bucket}/checkpoints"

# 3. READ SILVER STREAM FROM S3
df_silver      = spark.readStream.format("delta").load(silver_path)
df_watermarked = df_silver.withWatermark("event_timestamp", "10 minutes")

# 4. DAILY TRENDS
df_trends_stream = df_watermarked.groupBy(
    "coin_id",
    f.window("event_timestamp", "24 hours").alias("time_window")
).agg(
    f.avg("price_usd").alias("daily_avg_price"),
    f.max("price_usd").alias("daily_max_price"),
    f.min("price_usd").alias("daily_min_price"),
    f.avg("volume_24h").alias("daily_avg_volume")
).select(
    "coin_id",
    f.col("time_window.start").alias("window_start"),
    "daily_avg_price", "daily_max_price", "daily_min_price", "daily_avg_volume"
)

# 5. PRICE PERFORMANCE (sliding window)
df_perf_stream = df_watermarked.groupBy(
    "coin_id",
    f.window("event_timestamp", "5 minutes", "1 minute").alias("perf_window")
).agg(
    f.avg("price_usd").alias("moving_avg_price"),
    f.stddev("price_usd").alias("price_volatility")
).select(
    "coin_id",
    f.col("perf_window.start").alias("start_time"),
    "moving_avg_price", "price_volatility"
)

# 6. UPSERT FUNCTION FOR LATEST SNAPSHOT
def upsert_latest_snapshot(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    window_spec = Window.partitionBy("coin_id").orderBy(f.col("event_timestamp").desc())
    latest_in_batch = batch_df \
        .withColumn("rn", f.row_number().over(window_spec)) \
        .filter("rn = 1") \
        .drop("rn")
    latest_in_batch.write.format("delta").mode("overwrite").save(gold_path_snap)

# 7. WRITE ALL GOLD STREAMS TO S3
query1 = df_trends_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{checkpoint_base}/gold_trends") \
    .start(gold_path_trends)

query2 = df_perf_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", f"{checkpoint_base}/gold_perf") \
    .start(gold_path_perf)

query3 = df_silver.writeStream \
    .foreachBatch(upsert_latest_snapshot) \
    .option("checkpointLocation", f"{checkpoint_base}/gold_snapshot") \
    .start()

print(f"🏆 Gold Streams running! Writing trends, performance & snapshot to {s3_bucket}/gold/")
spark.streams.awaitAnyTermination()