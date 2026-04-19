"""
File        : bronze_validation.py
Location    : batch_pipeline/medallion/bronze/
Description : Performs comprehensive quality and integrity checks on raw Bronze
              layer data ingested from CoinGecko API into AWS S3.
              Acts as a quality gate before data moves to the Silver layer.

Input       : s3://crypto-lakehouse-neha/bronze/*.json
Output      : Console validation report (pass/fail per check)

Checks Performed:
    1. System-level JSON integrity (corrupt record detection)
    2. Schema contract validation (expected keys present)
    3. Data completeness (NULL price detection per coin)
    4. Metadata and lineage validation
    5. Data freshness (latest ingestion timestamp)

Dependencies:
    - pyspark
    - AWS S3 access configured

Warning:
    Requires active SparkSession. Run inside Databricks or
    an environment with PySpark and S3 access configured.
"""



from pyspark.sql import functions as F


# ── Configuration ─────────────────────────────────────────────────────────────
BRONZE_PATH    = "s3://crypto-lakehouse-neha/bronze/*.json"
EXPECTED_COINS = ["bitcoin", "ethereum", "solana"]
EXPECTED_KEYS  = EXPECTED_COINS + ["ingestion_metadata"]


# ── Load Bronze Data ──────────────────────────────────────────────────────────
df_bronze   = spark.read.option("multiLine", "true").json(BRONZE_PATH)
actual_keys = df_bronze.columns

print("─" * 60)
print("  BRONZE LAYER: COMPREHENSIVE QUALITY GATE")
print("─" * 60)


# ── Check 1: System-Level JSON Integrity ──────────────────────────────────────
total_raw = df_bronze.count()
print(f"\n[CHECK 1] System Integrity")
print(f"  Total records in Bronze : {total_raw}")

if "_corrupt_record" in actual_keys:
    corrupt_count = df_bronze.filter(F.col("_corrupt_record").isNotNull()).count()
    print(f"  CRITICAL : Corrupt JSON records found: {corrupt_count}")
else:
    print("  PASS     : No corrupt JSON records detected")


# ── Check 2: Schema Contract ──────────────────────────────────────────────────
print(f"\n[CHECK 2] Schema Contract")

missing_keys = [c for c in EXPECTED_KEYS if c not in actual_keys]
extra_keys   = [c for c in actual_keys if c not in EXPECTED_KEYS and c != "_corrupt_record"]

if missing_keys:
    print(f"  FAIL     : Missing required keys: {missing_keys}")
else:
    print("  PASS     : All expected keys present")

if extra_keys:
    print(f"  ALERT    : Unexpected new keys found (API change?): {extra_keys}")


# ── Check 3: Data Completeness ────────────────────────────────────────────────
print(f"\n[CHECK 3] Data Completeness")

for coin in EXPECTED_COINS:
    if coin in actual_keys:
        null_price = df_bronze.filter(F.col(f"{coin}.usd").isNull()).count()
        if null_price > 0:
            print(f"  FAIL     : {coin.capitalize()} has {null_price} record(s) with NULL price")
        else:
            print(f"  PASS     : {coin.capitalize()} — 100% complete")


# ── Check 4: Metadata & Lineage ───────────────────────────────────────────────
print(f"\n[CHECK 4] Metadata & Lineage")

invalid_meta = df_bronze.filter(
    F.col("ingestion_metadata.ingested_at").isNull() |
    (F.col("ingestion_metadata.ingested_at") == "")
).count()

if invalid_meta > 0:
    print(f"  FAIL     : {invalid_meta} record(s) missing ingestion timestamp")
else:
    print("  PASS     : Metadata lineage valid")


# ── Check 5: Data Freshness ───────────────────────────────────────────────────
print(f"\n[CHECK 5] Data Freshness")

try:
    latest_ingestion = df_bronze.select(
        F.max("ingestion_metadata.ingested_at")
    ).collect()[0][0]
    print(f"  INFO     : Latest ingestion timestamp: {latest_ingestion}")
except Exception as e:
    print(f"  WARNING  : Could not retrieve latest ingestion timestamp: {e}")

print("\n" + "─" * 60)
print("  BRONZE QUALITY GATE COMPLETE")
print("─" * 60)