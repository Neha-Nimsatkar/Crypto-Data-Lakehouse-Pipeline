import requests
import json
from datetime import datetime
from confluent_kafka import Producer
import socket

# 1. CONFIGURATION
COINS = "bitcoin,ethereum,solana"
CURRENCY = "usd"
BASE_URL = "https://api.coingecko.com/api/v3/simple/price"
KAFKA_TOPIC = "crypto_prices"
# Local Docker Kafka address
KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"

# 2. KAFKA PRODUCER SETUP (Simple Plaintext)
conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': socket.gethostname()
}

# Callback function to confirm delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def run_kafka_producer():
    try:
        print("INFO: Initializing Local Kafka Producer...")
        producer = Producer(conf)
        
        start_time = datetime.now()
        
        # --- FETCH ---
        params = {
            'ids': COINS, 'vs_currencies': CURRENCY,
            'include_market_cap': 'true', 'include_24hr_vol': 'true',
            'include_last_updated_at': 'true'
        }
        
        print(f"INFO: Fetching data from CoinGecko...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        data['ingestion_metadata'] = {
            "source": "CoinGecko API",
            "ingested_at": datetime.now().isoformat()
        }

        # --- PRODUCE ---
        print(f"INFO: Sending data to Local Kafka Topic: {KAFKA_TOPIC}...")
        json_payload = json.dumps(data)
        
        producer.produce(
            KAFKA_TOPIC, 
            value=json_payload, 
            callback=delivery_report
        )

        producer.flush()
        print(f"INFO: Completed in {(datetime.now() - start_time).total_seconds():.2f} seconds.")

    except Exception as e:
        print(f"❌ FAILED: {e}")

if __name__ == "__main__":
    run_kafka_producer()