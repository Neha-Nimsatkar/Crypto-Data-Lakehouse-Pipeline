"""
File        : crypto_api_fetch.py
Location    : batch_pipeline/ingestion/
Description : Fetches real-time cryptocurrency prices from the CoinGecko API
              and uploads raw JSON data to AWS S3 as the Bronze ingestion layer.

Input       : CoinGecko API (public, no auth required)
Output      : s3://<BUCKET_NAME>/bronze/batch_<timestamp>.json

Dependencies:
    - requests
    - boto3

Environment Variables Required (.env):
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_REGION
    - S3_BUCKET_NAME

Usage:
    python crypto_api_fetch.py

Warning:
    Never hardcode AWS credentials. Always load from environment variables.
"""

import os
import json
import requests
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ── Configuration ─────────────────────────────────────────────────────────────
COINS    = "bitcoin,ethereum,solana"
CURRENCY = "usd"
BASE_URL = "https://api.coingecko.com/api/v3/simple/price"

BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "crypto-lakehouse-neha")

# ── AWS Client ────────────────────────────────────────────────────────────────
s3 = boto3.client(
    "s3",
    aws_access_key_id     = os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name           = os.getenv("AWS_REGION", "us-east-1"),
)

# ── Ingestion ─────────────────────────────────────────────────────────────────
def run_ingestion():
    start_time = datetime.now()
    print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] INFO: Starting ingestion process...")

    params = {
        "ids"                  : COINS,
        "vs_currencies"        : CURRENCY,
        "include_market_cap"   : "true",
        "include_24hr_vol"     : "true",
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