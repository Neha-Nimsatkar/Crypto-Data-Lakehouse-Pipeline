from pyspark.sql import functions as f
import datetime

# 1. Load Raw Bronze Data (Using multiLine for pretty-printed JSON)
bronze_path = "s3://crypto-lakehouse-neha/bronze/*.json"
df_bronze = spark.read.option("multiLine", "true").json(bronze_path)

print(" --- BRONZE LAYER: COMPREHENSIVE SECURITY & QUALITY GATE ---")

# --- CHECK 1: System Level Integrity ---
total_raw = df_bronze.count()
print(f" Total Records in Bronze: {total_raw}")

if "_corrupt_record" in df_bronze.columns:
    corrupt_count = df_bronze.filter(f.col("_corrupt_record").isNotNull()).count()
    print(f" CRITICAL: Corrupt JSON Records Found: {corrupt_count}")
else:
    print(" JSON Structure: Valid (No corruption detected)")

# --- CHECK 2: Schema Contract (The 'Key' Check) ---
expected_keys = ["bitcoin", "ethereum", "solana", "ingestion_metadata"]
actual_keys = df_bronze.columns

missing_keys = [c for c in expected_keys if c not in actual_keys]
# EXTRA CHECK: Unexpected Columns (Catches if API adds new data we aren't handling)
extra_keys = [c for c in actual_keys if c not in expected_keys and c != "_corrupt_record"]

if missing_keys:
    print(f" SCHEMA DRIFT: Missing required keys: {missing_keys}")
else:
    print(" Schema Contract: Verified (All coin keys present)")

if extra_keys:
    print(f" SCHEMA ALERT: New/Unexpected keys found in JSON: {extra_keys}")

# --- CHECK 3: Data Completeness (Internal Field Check) ---
# We check if the 'usd' price exists inside every coin object
for coin in ["bitcoin", "ethereum", "solana"]:
    if coin in actual_keys:
        null_price = df_bronze.filter(f.col(f"{coin}.usd").isNull()).count()
        if null_price > 0:
            print(f" DATA GAP: {coin} has {null_price} records with NULL prices.")
        else:
            print(f" {coin.capitalize()} Data: 100% Complete")

# --- CHECK 4: Metadata & Lineage Check ---
invalid_meta = df_bronze.filter(
    (f.col("ingestion_metadata.ingested_at").isNull()) | 
    (f.col("ingestion_metadata.ingested_at") == "")
).count()

if invalid_meta > 0:
    print(f" LINEAGE ERROR: {invalid_meta} records missing ingestion timestamps!")
else:
    print(" Metadata Lineage: Valid")

# --- EXTRA CHECK 5: Data Freshness (Pre-Check) ---
# Helps identify if you are accidentally processing old files from weeks ago
# Extract the year from the ingestion string to ensure it's current
try:
    latest_ingestion = df_bronze.select(f.max("ingestion_metadata.ingested_at")).collect()[0][0]
    print(f" Latest Ingestion Timestamp in Batch: {latest_ingestion}")
except:
    print(" Could not calculate latest ingestion timestamp.")

print("----------------------------------------------------------")
