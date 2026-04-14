import os
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.window import Window
from delta import configure_spark_with_delta_pip

# 1. ENV SETUP
os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

AWS_ACCESS_KEY = "YOUR_ACCESS_KEY"
AWS_SECRET_KEY = "YOUR_SECRET_KEY"

builder = SparkSession.builder \
    .appName("Crypto_Gold_Streaming") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "2")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 2. PATHS (Silver is Local, Gold is Cloud)
silver_path = "A:/Crypto-Data-lakehouse-pipeline/data/silver/crypto_prices_clean"
s3_bucket = "s3a://crypto-lakehouse-neha"

gold_path_trends = f"{s3_bucket}/gold/daily_trends"
gold_path_perf   = f"{s3_bucket}/gold/price_performance"
gold_path_snap   = f"{s3_bucket}/gold/latest_snapshot"

df_silver = spark.readStream.format("delta").load(silver_path)
df_watermarked = df_silver.withWatermark("event_timestamp", "10 minutes")

# 🎯 Daily Trends
df_trends_stream = df_watermarked.groupBy(
    "coin_id", f.window("event_timestamp", "24 hours").alias("time_window")
).agg(
    f.avg("price_usd").alias("daily_avg_price"),
    f.max("price_usd").alias("daily_max_price"),
    f.min("price_usd").alias("daily_min_price"),
    f.avg("volume_24h").alias("daily_avg_volume")
).select(
    "coin_id", f.col("time_window.start").alias("window_start"),
    "daily_avg_price", "daily_max_price", "daily_min_price", "daily_avg_volume"
)

# 🎯 Performance
df_perf_stream = df_watermarked.groupBy(
    "coin_id", f.window("event_timestamp", "5 minutes", "1 minute").alias("perf_window")
).agg(
    f.avg("price_usd").alias("moving_avg_price"),
    f.stddev("price_usd").alias("price_volatility")
).select(
    "coin_id", f.col("perf_window.start").alias("start_time"),
    "moving_avg_price", "price_volatility"
)

# 🎯 Upsert Function
def upsert_latest_snapshot(batch_df, batch_id):
    window_spec = Window.partitionBy("coin_id").orderBy(f.col("event_timestamp").desc())
    latest_in_batch = batch_df.withColumn("rn", f.row_number().over(window_spec)) \
                              .filter("rn = 1").drop("rn")
    
    latest_in_batch.write.format("delta").mode("overwrite").save(gold_path_snap)

# 🚀 WRITE TO GOLD S3
query1 = df_trends_stream.writeStream \
    .format("delta").outputMode("append") \
    .option("checkpointLocation", f"{s3_bucket}/checkpoints/gold_trends") \
    .start(gold_path_trends)

query2 = df_perf_stream.writeStream \
    .format("delta").outputMode("append") \
    .option("checkpointLocation", f"{s3_bucket}/checkpoints/gold_perf") \
    .start(gold_path_perf)

query3 = df_silver.writeStream \
    .foreachBatch(upsert_latest_snapshot) \
    .option("checkpointLocation", f"{s3_bucket}/checkpoints/gold_snapshot") \
    .start()

print("🏆 Gold Streams are running in S3!")
spark.streams.awaitAnyTermination()