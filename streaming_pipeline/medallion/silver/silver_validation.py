import os
from pyspark.sql import SparkSession, functions as f
from delta import configure_spark_with_delta_pip
import datetime

# 1. ENV SETUP
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
    .appName("Silver_Validation_Check") \
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

# 2. ✅ S3 Silver Path
silver_path = "s3a://crypto-lakehouse-neha/silver/crypto_prices_clean"

try:
    df_check = spark.read.format("delta").load(silver_path)

    print("\n" + "="*50)
    print("🔍 SILVER LAYER: FINAL STREAMING VALIDATION")
    print("="*50)

    # --- 1. INTEGRITY CHECKS ---
    duplicate_count = df_check.groupBy("coin_id", "event_timestamp").count().filter("count > 1").count()
    print(f"✅ Duplicate Records (expected 0): {duplicate_count}")

    null_prices = df_check.filter(f.col("price_usd").isNull()).count()
    print(f"✅ Records with Null Prices (expected 0): {null_prices}")

    # --- 2. STORAGE & PARTITIONING ---
    coins_found = [row['coin_id'] for row in df_check.select("coin_id").distinct().collect()]
    print(f"✅ Coins trackable in Silver: {coins_found}")

    # --- 3. FRESHNESS CHECK ---
    latest_row = df_check.select(f.max("event_timestamp")).collect()[0][0]
    if latest_row:
        delay_mins = (datetime.datetime.now() - latest_row).total_seconds() / 60
        print(f"✅ Freshness Check: Data is {round(delay_mins, 2)} minutes old.")
    else:
        print("⚠️ Freshness Check: No data found in Silver yet.")

    # --- 4. VOLUME CHECK ---
    print("\n📊 Volume Check (Rows per Coin):")
    df_check.groupBy("coin_id").count().show()

    # --- 5. ANOMALY DETECTION ---
    outliers = df_check.filter(
        (f.col("coin_id") == "bitcoin") &
        ((f.col("price_usd") < 20000) | (f.col("price_usd") > 200000))
    ).count()
    if outliers > 0:
        print(f"❌ ANOMALY: Detected {outliers} suspicious price points for Bitcoin!")
    else:
        print("✅ Reasonability Check: Bitcoin price range is valid.")

    # --- 6. SAMPLE VIEW ---
    print("\n👀 Latest 5 Records in Silver:")
    df_check.orderBy(f.col("event_timestamp").desc()).show(5)

    print("="*50 + "\n")

except Exception as e:
    print(f"❌ ERROR loading Silver data from S3: {e}")