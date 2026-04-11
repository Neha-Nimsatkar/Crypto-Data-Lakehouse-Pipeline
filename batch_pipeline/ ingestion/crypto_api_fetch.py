import requests
import json
import os
import time
from datetime import datetime

# Configuration
# CoinGecko doesn't need an API key for this public endpoint
COINS = "bitcoin,ethereum,solana"
CURRENCY = "usd"
BASE_URL = "https://api.coingecko.com/api/v3/simple/price"
BRONZE_LAYER_PATH = "data/bronze"

# Ensure our "Data Lake" folder exists
os.makedirs(BRONZE_LAYER_PATH, exist_ok=True)

def fetch_crypto_prices():
    """Fetches real-time prices from CoinGecko API."""
    params = {
        'ids': COINS,
        'vs_currencies': CURRENCY,
        'include_market_cap': 'true',
        'include_24hr_vol': 'true',
        'include_last_updated_at': 'true'
    }
    
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching data...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status() # Raise error for bad status codes
        return response.json()
    except Exception as e:
        print(f" API Error: {e}")
        return None

def save_to_local_lake(data):
    """Saves the raw JSON data to our Bronze layer."""
    # Create a unique filename using a timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(BRONZE_LAYER_PATH, f"batch_{timestamp}.json")
    
    # Add a metadata field to track when we ingested it
    data['ingestion_metadata'] = {
        "source": "CoinGecko API",
        "ingested_at": datetime.now().isoformat()
    }

    with open(file_path, 'w') as f:
        json.dump(data, f, indent=4)
    
    print(f" Successfully saved to: {file_path}")

def main():
    print(" Starting Crypto Data Ingestion (Phase 1)...")
    print(f"Target Coins: {COINS.upper()}")
    print("Press Ctrl+C to stop.")
    
    try:
        while True:
            raw_data = fetch_crypto_prices()
            if raw_data:
                save_to_local_lake(raw_data)
            
            # Wait 30 seconds (CoinGecko free tier has rate limits)
            time.sleep(30)
    except KeyboardInterrupt:
        print("\n Ingestion stopped by user.")

if __name__ == "__main__":
    main()
