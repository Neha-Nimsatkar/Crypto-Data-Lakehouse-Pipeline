import os
import sys

# 1. SET JAVA 11 ENVIRONMENT (Using the path you found)
java_home = r"C:\Program Files\Amazon Corretto\jdk11.0.30_7"

if os.path.exists(java_home):
    os.environ["JAVA_HOME"] = java_home
    # This adds the Java bin folder to the start of your PATH for this script only
    os.environ["PATH"] = os.path.join(java_home, "bin") + os.path.pathsep + os.environ["PATH"]
    print(f"✅ Java 11 Environment Set: {java_home}")
else:
    print(f"❌ ERROR: Path not found: {java_home}")
    sys.exit(1)

# 2. START SPARK SESSION
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

print("INFO: Initializing Spark Session...")

# Note: Using spark-sql-kafka 0-10 and Delta 2.4.0 (stable with Spark 3.4)
spark = SparkSession.builder \
    .appName("CryptoStreamingBronze") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.hadoop.security.ignore.getSubject.error", "true") \
    .getOrCreate()

# 3. DEFINE THE DATA SCHEMA
schema = StructType([
    StructField("bitcoin", StructType([StructField("usd", DoubleType())])),
    StructField("ethereum", StructType([StructField("usd", DoubleType())])),
    StructField("solana", StructType([StructField("usd", DoubleType())])),
    StructField("ingestion_metadata", StructType([
        StructField("source", StringType()),
        StructField("ingested_at", StringType())
    ]))
])

# 4. READ FROM KAFKA
print("INFO: Connecting to Kafka...")
raw_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "crypto_prices") \
    .option("startingOffsets", "earliest") \
    .load()

# 5. TRANSFORM DATA
parsed_df = raw_stream_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 6. WRITE TO BRONZE DELTA TABLE
checkpoint_path = "A:/Crypto-Data-lakehouse-pipeline/checkpoints/bronze_ingestion"
bronze_path = "A:/Crypto-Data-lakehouse-pipeline/data/bronze/crypto_prices_delta"

print(f"🚀 Streaming Started... Saving data to {bronze_path}")

query = parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_path) \
    .start(bronze_path)

query.awaitTermination()