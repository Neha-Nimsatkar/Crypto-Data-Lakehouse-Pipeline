"""
File        : crypto_api_fetch.py
Location    : batch_pipeline/ingestion/
Description : Fetches real-time cryptocurrency prices from the CoinGecko API
              and uploads raw JSON data to AWS S3 as the Bronze ingestion layer.
              Updated for Databricks Free Edition Serverless Execution compatibility.

Input       : CoinGecko API (public, no auth required)
Output      : s3://crypto-lakehouse-nehaa/bronze/batch_<timestamp>.json
"""

import os
import json
import sys
import requests
import boto3
from datetime import datetime

# ── Step 1: Parse AWS Credentials Securely from Environment ──────────────────
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

# Local testing fallback if running on local terminal
if not AWS_KEY or not AWS_SECRET:
    try:
        from dotenv import load_dotenv
        load_dotenv()
        AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
        AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
    except ImportError:
        print("INFO: Local dotenv module not found, relying on raw system environment vars.")

# ── Step 2: Initialize Authenticated AWS S3 Client ───────────────────────────
if AWS_KEY and AWS_SECRET:
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
else:
    # Industry standard fallback for IAM Roles / Cluster policies
    print("WARNING: AWS keys not found in environment. Initializing generic fallback client.")
    s3 = boto3.client('s3')

# ── Step 3: Global Configurations ─────────────────────────────────────────────
BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "crypto-lakehouse-nehaa")
COINS = (
    "bitcoin,ethereum,solana,ripple,cardano,dogecoin,polkadot,polygon,"
    "shiba-inu,avalanche-2,chainlink,uniswap,litecoin,stellar,near"
)
CURRENCY = "usd"
BASE_URL = "https://api.coingecko.com/api/v3/simple/price"

# ── Step 4: Core Ingestion Logic ──────────────────────────────────────────────
def run_ingestion():
    start_time = datetime.now()
    print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] INFO: Starting ingestion process...")

    params = {
        "ids"                     : COINS,
        "vs_currencies"           : CURRENCY,
        "include_market_cap"      : "true",
        "include_24hr_vol"        : "true",
        "include_last_updated_at": "true",
    }

    try:
        print(f"INFO: Fetching data for coins: {COINS}...")
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print("SUCCESS: Data fetched from CoinGecko API.")

        # Ingestion metadata integration
        data["ingestion_metadata"] = {
            "source"      : "CoinGecko API",
            "ingested_at" : datetime.now().isoformat(),
            "coins"       : COINS,
            "currency"    : CURRENCY,
        }

        # Upload JSON array snapshot payload directly to S3 Bucket
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
