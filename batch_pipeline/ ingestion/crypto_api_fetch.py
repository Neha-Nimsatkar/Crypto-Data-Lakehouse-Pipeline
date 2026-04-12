import requests
import json
import boto3
from datetime import datetime
import io

# 1. CONFIGURATION
COINS = "bitcoin,ethereum,solana"
CURRENCY = "usd"
BASE_URL = "https://api.coingecko.com/api/v3/simple/price"
BUCKET_NAME = 'crypto-lakehouse-neha'

# 2. AWS SETUP
# 2. AWS SETUP
s3 = boto3.client(
    's3',
    aws_access_key_id='AKIAWYKG6KACJLEYZI65', 
    aws_secret_access_key='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'
)

def run_ingestion():
    # --- START TRACE ---
    start_time = datetime.now()
    print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] INFO: Starting ingestion process...")

    # --- FETCH ---
    params = {
        'ids': COINS, 'vs_currencies': CURRENCY,
        'include_market_cap': 'true', 'include_24hr_vol': 'true',
        'include_last_updated_at': 'true'
    }
    
    try:
        print(f"INFO: Fetching data for coins: {COINS}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        print("SUCCESS: Data successfully fetched from CoinGecko API.")
        
        # Add Metadata
        data['ingestion_metadata'] = {
            "source": "CoinGecko API",
            "ingested_at": datetime.now().isoformat()
        }

        # --- UPLOAD DIRECTLY TO S3 (Streaming) ---
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        s3_key = f"bronze/batch_{timestamp}.json"
        
        # Convert dictionary to JSON string
        json_data = json.dumps(data, indent=4)
        
        print(f"INFO: Attempting to upload to S3 bucket: {BUCKET_NAME} at key: {s3_key}...")
        
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=json_data
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # --- FINAL SUCCESS MESSAGE ---
        print("-" * 50)
        print(f" FINAL STATUS: SUCCESS")
        print(f"FILE CREATED: s3://{BUCKET_NAME}/{s3_key}")
        print(f"TIME COMPLETED: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"TOTAL DURATION: {duration:.2f} seconds")
        print("-" * 50)

    except requests.exceptions.HTTPError as http_err:
        print(f" API ERROR: HTTP error occurred: {http_err}")
        raise
    except Exception as e:
        print(f" FAILED: An unexpected error occurred: {e}")
        raise e

if __name__ == "__main__":
    run_ingestion()