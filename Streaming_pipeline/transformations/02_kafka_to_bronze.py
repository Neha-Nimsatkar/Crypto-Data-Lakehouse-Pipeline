import sys
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- STEP 1: INITIALIZE ENVIRONMENT PATHS ---
CHECKPOINT_PATH = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/bronze_table/"

try:
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    print("🧹 Old incompatible checkpoints cleared!")
except Exception as e:
    pass

# ── 🔐 STEP 2: DIRECT EXTRACTION FROM SECURE TASK PARAMETERS ARRAY ──
try:
    # sys.argv[0] is the script path. 1, 2, 3 are your Confluent credentials.
    BOOTSTRAP_SERVER = sys.argv[1].replace('"', '').replace("'", "").strip()
    API_KEY          = sys.argv[2].replace('"', '').replace("'", "").strip()
    API_SECRET       = sys.argv[3].replace('"', '').replace("'", "").strip()
    TOPIC_NAME       = "crypto_market_ticks"
    
    print("🔒 Confluent Cloud credentials successfully parsed from secure array injection!")
except IndexError:
    print("❌ Critical: Please ensure you have passed exactly 3 strings in the Task Parameter array.")
    raise

# Confluent Shaded Configuration Matrix for Serverless Compute Runtime
jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{API_KEY}\" password=\"{API_SECRET}\";"

# ── STEP 3: SPARK STRUCTURED STREAMING READ ──
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    
    # Serverless Shaded Core Protocols Handshake Settings
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
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

# --- STEP 6: LIVE STREAM SINK TO DELTA TABLE ---
query = (parsed_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.default.crypto_bronze_table"))

query.awaitTermination()
print("🏆 Victory! Stream successfully processed and data landed into Delta Lakehouse!")
