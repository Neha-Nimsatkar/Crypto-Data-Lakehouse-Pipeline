"""
fetches crypto prices from CoinGecko for 15 coins
saves raw JSON to S3 bronze layer
uses GitHub Actions secrets on cloud, dotenv locally

"""


import os
import json
import sys
import requests
import boto3
from datetime import datetime

try:
    from databricks.sdk.runtime import dbutils
    AWS_KEY = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="aws_id")
    AWS_SECRET = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="aws_secret")
except Exception:
    AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not AWS_KEY or not AWS_SECRET:
        try:
            from dotenv import load_dotenv
            load_dotenv()
            AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
            AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")
        except ImportError:
            print(" Local dotenv module not found, relying on raw system environment vars.")


if AWS_KEY and AWS_SECRET:
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET
    )
else:
    print(" AWS keys not found in environment. Initializing generic fallback client.")
    s3 = boto3.client('s3')


BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "crypto-lakehouse-nehaa")
COINS = (
    "bitcoin,ethereum,solana,ripple,cardano,dogecoin,polkadot,polygon,"
    "shiba-inu,avalanche-2,chainlink,uniswap,litecoin,stellar,near"
)
CURRENCY = "usd"
BASE_URL = "https://api.coingecko.com/api/v3/simple/price"

# 
def run_ingestion():
    start_time = datetime.now()
    print(f"starting ingestion at {start_time.strftime('%Y-%m-%d %H:%M:%S')}")


    params = {
        "ids": COINS,
        "vs_currencies" : CURRENCY,
        "include_market_cap": "true",
        "include_24hr_vol": "true",
        "include_last_updated_at": "true",
    }

    try:
        print(f"fetching prices for 15 coins...")
        response = requests.get(BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print("Data fetched from CoinGecko API.")

      
        data["ingestion_metadata"] = {
            "source" : "CoinGecko API",
            "ingested_at" : datetime.now().isoformat(),
            "coins": COINS,
            "currency": CURRENCY,
        }


        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key    = f"bronze/batch_{timestamp}.json"
        json_data = json.dumps(data, indent=4)

        print(f"Uploading to s3://{BUCKET_NAME}/{s3_key} ...")
        s3.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=json_data)

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

       
        print(f"Fetching Successfully Completed")
        print(f" File : s3://{BUCKET_NAME}/{s3_key}")
        print(f" Completed : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f" Duration: {duration:.2f} seconds")
        


    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        raise
    except requests.exceptions.ConnectionError:
        print("Could not connect to CoinGecko API. Check internet connection.")
        raise
    except requests.exceptions.Timeout:
        print("Request timed out after 30 seconds.")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise


if __name__ == "__main__":
    run_ingestion()