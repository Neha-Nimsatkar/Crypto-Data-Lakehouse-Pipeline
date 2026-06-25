
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType



try:
    # Purane dynamic offsets folder ko completely fresh clear karne ke liye
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    print(" Old incompatible checkpoints successfully cleared!")
except Exception as e:
    print(" Checkpoint directory already clean or fresh.")

# ── STEP 1.5: DEFINE WIDGET PLACEHOLDERS FOR WORKFLOW INHERITANCE ──
# Yeh lines Databricks Workflow ko batayengi ki widgets runtime par defined hain!
dbutils.widgets.text("kafka_bootstrap_server", "")
dbutils.widgets.text("kafka_api_key", "")
dbutils.widgets.text("kafka_api_secret", "")

# ── STEP 2: CONFIGURATION FETCHING ────────────────────────────────
BOOTSTRAP_SERVER = dbutils.widgets.get("kafka_bootstrap_server")
API_KEY          = dbutils.widgets.get("kafka_api_key")
API_SECRET       = dbutils.widgets.get("kafka_api_secret")
TOPIC_NAME = "crypto_market_ticks"

# Shaded prefix for Serverless Compute active
jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{API_KEY}' password='{API_SECRET}';"

# --- STREAM READER ---
kafka_stream_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", jaas_config) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# --- SCHEMA DEFINITION ---
crypto_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

parsed_stream_df = kafka_stream_df \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), crypto_schema).alias("data")) \
    .select("data.*")

# --- STEP 1: DEFINE PERMANENT BRONZE STORAGE PATHS ---
# Alag state checkpoints folder for our permanent Bronze Table
BRONZE_CHECKPOINT = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/bronze_table/"

# --- STEP 2: LIVE STREAM SINK TO DELTA TABLE (THE REAL BRONZE LAYER) ---
# Format badalkar "delta" kiya aur .toTable() se complete catalog bind kar diya
# --- STEP 2: LIVE STREAM SINK TO DELTA TABLE (THE REAL BRONZE LAYER) ---
query = parsed_stream_df.writeStream \
    .format("delta") \
    .option("checkpointLocation", BRONZE_CHECKPOINT) \
    .outputMode("append") \
    .trigger(availableNow=True) \
    .toTable("workspace.default.crypto_bronze_table")

# ── MAGIC LINE: Enforces the script to wait until the execution stream safely ends ──
query.awaitTermination()

print(" Raw Kafka messages successfully persisted into permanent Bronze Delta Table!")
