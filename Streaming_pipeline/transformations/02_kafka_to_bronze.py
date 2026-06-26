import os
import sys
import json
import boto3
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- STEP 1: INITIALIZE ENVIRONMENT PATHS ---
CHECKPOINT_PATH = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/bronze_table/"

try:
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    print("Old incompatible checkpoints cleared!")
except Exception as e:
    pass

# ──  STEP 2: PARSE ARRAY STRINGS & INJECT RUNTIME ENVIRONMENT ──
# Databricks Task Parameters pass array strings via standard sys.argv array
try:
    # sys.argv[0] script name hota hai, array ke elements index 1 aur 2 par aate hain
    AWS_KEY = sys.argv[1].replace('"', '').replace("'", "").strip()
    AWS_SEC = sys.argv[2].replace('"', '').replace("'", "").strip()
    
    # Explicit mapping into local python machine process dictionary
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_KEY
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SEC
    os.environ["AWS_DEFAULT_REGION"] = "ap-south-1"  # Mumbai Region matching your console
    print(" AWS Runtime authorization variables successfully injected!")
except IndexErorr:
    print(" Critical: Please make sure you have passed exactly 2 strings in the Task Parameter array.")
    raise

def get_crypto_secrets():
    secret_name = "crypto/confluent/keys"
    region_name = "ap-south-1"

    # Boto3 implicitly maps AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from os.environ
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = get_secret_value_response['SecretString']
        return json.loads(secret)
    except Exception as e:
        print(f" Failed to fetch secrets from AWS: {str(e)}")
        raise e

# Fetching the clean dictionary directly from AWS Secrets Manager
aws_secrets = get_crypto_secrets()

BOOTSTRAP_SERVER = aws_secrets["kafka_bootstrap_server"].strip()
API_KEY          = aws_secrets["kafka_api_key"].strip()
API_SECRET       = aws_secrets["kafka_api_secret"].strip()
TOPIC_NAME       = "crypto_market_ticks"

# Confluent Shaded Specification String Configuration for Serverless Compute Runtime
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
print(" AWS Secrets backed stream successfully processed and landed in Bronze Layer!")
