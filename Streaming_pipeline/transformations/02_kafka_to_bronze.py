from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- STEP 1: INITIALIZE ENVIRONMENT PATHS ---
CHECKPOINT_PATH = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/bronze_table/"

try:
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    print("🧹 Old incompatible checkpoints successfully cleared!")
except Exception as e:
    print("🔄 Checkpoint directory already clean or fresh.")

# ── STEP 1.5: DEFINE WIDGET PLACEHOLDERS FOR WORKFLOW INHERITANCE ──
dbutils.widgets.text("kafka_bootstrap_server", "")
dbutils.widgets.text("kafka_api_key", "")
dbutils.widgets.text("kafka_api_secret", "")

# ── STEP 2: CONFIGURATION FETCHING WITH STRIP FIX ──
# .strip('\'"') lagane se JSON waale outer quotes python variable se delete ho jayenge
BOOTSTRAP_SERVER = dbutils.widgets.get("kafka_bootstrap_server").strip('\'"')
API_KEY          = dbutils.widgets.get("kafka_api_key").strip('\'"')
API_SECRET       = dbutils.widgets.get("kafka_api_secret").strip('\'"')
TOPIC_NAME       = "crypto_market_ticks"

# Confluent Cloud Security Configuration with clean parsed variables
jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{API_KEY}\" password=\"{API_SECRET}\";"

# ── STEP 3: SPARK STRUCTURED STREAMING READ FROM CONFLUENT KAFKA ──
# ── STEP 3: SPARK STRUCTURED STREAMING READ FROM CONFLUENT KAFKA ──
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    
    # Standard Security Configurations
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    
    # CRITICAL CONFLUENT FIX: Ye do lines ensure karengi ki handshake reject na ho
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .option("kafka.client.dns.lookup", "use_all_dns_ips")
    .load())

# --- STEP 4: SCHEMA DEFINITION ---
crypto_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])

# --- STEP 5: PARSE RAW KAFKA PAYLOAD ---
parsed_stream_df = (kafka_df
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), crypto_schema).alias("data"))
    .select("data.*"))

# --- STEP 6: LIVE STREAM SINK TO DELTA TABLE (BRONZE LAYER) ---
query = (parsed_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.default.crypto_bronze_table"))

query.awaitTermination()

print("🚀 Raw Confluent Kafka messages successfully clean-parsed and saved into Bronze Delta Table!")
