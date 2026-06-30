# pulls ticks off the kafka topic and lands them raw into the bronze delta table
# bronze = first stop in the medallion setup, no transforms here yet
# uses confluent cloud as the kafka source

import sys
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType


CHECKPOINT_PATH = "/Volumes/workspace/default/crypto_silver_volume/checkpoints/bronze_table/"

try:
    dbutils.fs.rm(CHECKPOINT_PATH, recurse=True)
except Exception as e:
    pass


try:
    from databricks.sdk.runtime import dbutils
    BOOTSTRAP_SERVER = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="confluent_bootstrap_server")
    API_KEY          = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="confluent_api_key")
    API_SECRET       = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="confluent_api_secret")
    TOPIC_NAME       = "crypto_market_ticks"

    print("got the confluent creds from the secrets scope, good to go")
except Exception as e:
    print(f"couldn't pull creds from secrets scope: {e}")
    raise


jaas_config = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{API_KEY}\" password=\"{API_SECRET}\";"


kafka_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVER)
    .option("subscribe", TOPIC_NAME)
    .option("startingOffsets", "earliest")
    
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", jaas_config)
    .option("kafka.ssl.endpoint.identification.algorithm", "https")
    .load())


crypto_schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("timestamp", LongType(), True)
])


parsed_stream_df = (kafka_df
    .selectExpr("CAST(value AS STRING) as json_payload")
    .select(from_json(col("json_payload"), crypto_schema).alias("data"))
    .select("data.*"))


query = (parsed_stream_df.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.default.crypto_bronze_table"))


print("batch landed in bronze, done")