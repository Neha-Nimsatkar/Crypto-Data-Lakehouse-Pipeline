import boto3
import json
from botocore.exceptions import ClientError
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- STEP 1: INITIALIZE ENVIRONMENT PATHS ---
CHECKPOINT_PATH = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/bronze_table/"

try:
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
    print("🧹 Old incompatible checkpoints cleared!")
except Exception as e:
    pass

# ── 🔐 STEP 2: SECURE FETCH VIA AWS SECRETS MANAGER ──
def get_crypto_secrets():
    secret_name = "crypto/confluent/keys"
    region_name = "ap-south-1"  # Tumhara exact AWS region jahan secret saved hai

    # Create a Secrets Manager client
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
        print(f"❌ Failed to fetch secrets from AWS: {str(e)}")
        raise e

# Fetching the clean dictionary directly from AWS KMS
aws_secrets = get_crypto_secrets()

BOOTSTRAP_SERVER = aws_secrets["kafka_bootstrap_server"].strip()
API_KEY          = aws_secrets["kafka_api_key"].strip()
API_SECRET       = aws_secrets["kafka_api_secret"].strip()
TOPIC_NAME       = "crypto_market_ticks"

# Explicit escaping of strings inside JAAS format
jaas_config = f"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{API_KEY}\" password=\"{API_SECRET}\";"

# ── STEP 3: SPARK STRUCTURED STREAMING READ ──
kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "latest")
    
    # Secure Direct Cloud-Injected Properties
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
print("🚀 AWS Secrets backed stream successfully processed and landed in Bronze Layer!")
