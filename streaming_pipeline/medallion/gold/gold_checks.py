import os
from pyspark.sql import SparkSession, functions as f
from delta import configure_spark_with_delta_pip

os.environ["JAVA_HOME"] = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"
os.environ["HADOOP_HOME"] = r"C:\hadoop"

# ✅ Force S3A JARs to load before session starts
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages io.delta:delta-core_2.12:2.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.200 pyspark-shell"
)

AWS_ACCESS_KEY = "AKIAWYKG6KACJLEYZI65"
AWS_SECRET_KEY = "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"

builder = SparkSession.builder \
    .appName("Gold_Quality_Checks") \
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

# ✅ Fixed typo: "io.dchange elta" → "io.delta" (was broken in original)
# All paths point to S3
s3_bucket     = "s3a://crypto-lakehouse-neha"
silver_path   = f"{s3_bucket}/silver/crypto_prices_clean"
path_perf     = f"{s3_bucket}/gold/price_performance"
path_trends   = f"{s3_bucket}/gold/daily_trends"
path_snapshot = f"{s3_bucket}/gold/latest_snapshot"

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df_gold_perf   = spark.read.format("delta").load(path_perf)
df_gold_trends = spark.read.format("delta").load(path_trends)
df_gold_snap   = spark.read.format("delta").load(path_snapshot)