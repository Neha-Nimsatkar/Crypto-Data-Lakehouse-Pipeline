"""
File        : crypto_api_fetch.py
Location    : batch_pipeline/ingestion/
Description : Fetches real-time cryptocurrency prices from the CoinGecko API
              and uploads raw JSON data to AWS S3 as the Bronze ingestion layer.
              Updated for Databricks cloud orchestration native compatibility.

Input       : CoinGecko API (public, no auth required)
Output      : s3://crypto-lakehouse-nehaa/bronze/batch_<timestamp>.json
"""

import os
import json
import requests
import boto3
from datetime import datetime

# ── UPGRADE: Databricks Workflow Parameters Handling ─────────────────────────
try:
    from pyspark.dbutils import DBUtils
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    # Databricks Task parameter se parameters extract karega
    BUCKET_NAME = dbutils.widgets.get("s3_bucket_name")
except Exception:
    # Fallback option: local execution via .env testing
    from dotenv import load_dotenv
    load_dotenv()
    BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "crypto-lakehouse-nehaa")

# ── Configuration ─────────────────────────────────────────────────────────────
COINS = (
    "bitcoin,ethereum,solana,ripple,cardano,dogecoin,polkadot,polygon,"
    "shiba-inu,avalanche-2,chainlink,uniswap,litecoin,stellar,near"
)
CURRENCY = "usd"
BASE_URL = "https://api.coingecko.com/api/v3/simple/price"

# ── AWS Client (Upgraded for Cloud Security Standards) ───────────────────────
# Explicit hardcoded keys bypass karke dynamic underlying IAM instance profile policy utilize karega
s3 = boto3.client("s3")

# ── Ingestion ─────────────────────────────────────────────────────────────────
def run_ingestion():
    start_time = datetime.now()
    print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] INFO: Starting ingestion process...")

    params = {
        "ids"                     : COINS,
        "vs_currencies"          : CURRENCY,
        "include_market_cap"     : "true",
        "include_24hr_vol"       : "true",
        "include_last_updated_at": "true",
    }

    try:
        print(f"INFO: Fetching data for coins: {COINS}...")
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print("SUCCESS: Data fetched from CoinGecko API.")

        # Add ingestion metadata
        data["ingestion_metadata"] = {
            "source"      : "CoinGecko API",
            "ingested_at" : datetime.now().isoformat(),
            "coins"       : COINS,
            "currency"    : CURRENCY,
        }

        # Upload to S3 Bronze layer
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key    = f"bronze/batch_{timestamp}.json"
        json_data = json.dumps(data, indent=4)

        print(f"INFO: Uploading to s3://{BUCKET_NAME}/{s3_key} ...")
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=json_data)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        print("-" * 50)
        print(f"  STATUS    : SUCCESS")
        print(f"  FILE      : s3://{BUCKET_NAME}/{s3_key}")
        print(f"  COMPLETED : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  DURATION  : {duration:.2f} seconds")
        print("-" * 50)

    except requests.exceptions.HTTPError as http_err:
        print(f"API ERROR : HTTP error occurred: {http_err}")
        raise
    except requests.exceptions.ConnectionError:
        print("API ERROR : Could not connect to CoinGecko API. Check internet connection.")
        raise
    except requests.exceptions.Timeout:
        print("API ERROR : Request timed out after 30 seconds.")
        raise
    except Exception as e:
        print(f"FAILED    : Unexpected error: {e}")
        raise


# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_ingestion()