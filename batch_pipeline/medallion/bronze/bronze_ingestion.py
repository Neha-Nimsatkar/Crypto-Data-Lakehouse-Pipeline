
# 1. Load Raw Bronze Data (Using multiLine for pretty-printed JSON)
bronze_path = "s3://crypto-lakehouse-neha/bronze/*.json"
df_bronze = spark.read.option("multiLine", "true").json(bronze_path)

print(" --- BRONZE LAYER: COMPREHENSIVE SECURITY & QUALITY GATE ---")
