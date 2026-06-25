from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- STEP 1: INITIALIZE ENVIRONMENT PATHS ---
# Tumhara dynamic check parameter jo tum clean kar rahi ho
# (Dhyan rakhna ki CHECKPOINT_PATH variable script mein upar ya global defined ho, 
# nahi toh tum ise seedhe BRONZE_CHECKPOINT se bhi overwrite kar sakti ho)
CHECKPOINT_PATH = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/bronze_table/"

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
TOPIC_NAME       = "crypto_market_ticks"

# Shaded prefix for Serverless Compute active
# FIX: Internally explicit double quotes ("") lagaye hain username aur password par escape format ke sath
jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{API_KEY}\" password=\"{API_SECRET}\";"

# ── STEP 3: SPARK STRUCTURED STREAMING READ FROM AIVEN KAFKA ──
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    
    # Security Configurations 
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)  
    .load())

# --- STEP 4: SCHEMA DEFINITION ---
crypto_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# --- STEP 5: PARSE RAW KAFKA PAYLOAD ---
# FIX: 'kafka_stream_df' ko badal kar 'kafka_df' kiya jo upar defined hai
parsed_stream_df = (kafka_df
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), crypto_schema).alias("data"))
    .select("data.*"))

# --- STEP 6: LIVE STREAM SINK TO DELTA TABLE (THE REAL BRONZE LAYER) ---
query = (parsed_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)  # Batch-like execution cycle for workflows
    .toTable("workspace.default.crypto_bronze_table"))

# ── MAGIC LINE: Enforces the script to wait until the execution stream safely ends ──
query.awaitTermination()

print(" Raw Kafka messages successfully persisted into permanent Bronze Delta Table!")
