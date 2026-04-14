import requests
import json
from datetime import datetime
from confluent_kafka import Producer
import socket
import time

# 1. CONFIGURATION
COINS = "bitcoin,ethereum,solana"
CURRENCY = "usd"
BASE_URL = "https://api.coingecko.com/api/v3/simple/price"
KAFKA_TOPIC = "crypto_prices"
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': socket.gethostname()
}

def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def run_kafka_producer():
    print("INFO: Initializing Local Kafka Producer...")
    producer = Producer(conf)
    print(f"🚀 Starting Continuous Stream (Press Ctrl+C to stop)...")
    
    try:
        while True:
            # --- FETCH ---
            params = {
                'ids': COINS, 
                'vs_currencies': CURRENCY,
                'include_market_cap': 'true', 
                'include_24hr_vol': 'true',
                'include_last_updated_at': 'true'
            }
            
            try:
                response = requests.get(BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()
                
                # Add ingestion metadata
                data['ingestion_metadata'] = {
                    "source": "CoinGecko API",
                    "ingested_at": datetime.now().isoformat()
                }

                # --- PRODUCE ---
                json_payload = json.dumps(data)
                producer.produce(
                    KAFKA_TOPIC, 
                    key="crypto_update", # Added a key for better partitioning
                    value=json_payload, 
                    callback=delivery_report
                )
                producer.flush()
                
                print(f"📡 Data sent at {datetime.now().strftime('%H:%M:%S')}")
                
            except Exception as e:
                print(f"⚠️ Fetch Error: {e}")

            time.sleep(60) 

    except KeyboardInterrupt:
        print("\n🛑 Producer stopped by user.")

if __name__ == "__main__":
    run_kafka_producer()