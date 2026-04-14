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
AIVEN_SERVICE_URI = "kafka-16a146bd-cryptoproject123.k.aivencloud.com:10109"
# 2. KAFKA PRODUCER SETUP
conf = {
    'bootstrap.servers': AIVEN_SERVICE_URI,
    'security.protocol': 'SSL',
    'ssl.ca.location': 'ca.pem',
    'ssl.certificate.location': 'service.cert',
    'ssl.key.location': 'service.key',
    'client.id': socket.gethostname()
}

producer = Producer(conf)

# Callback function to confirm delivery
def delivery_report(err, msg):
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def run_kafka_producer():
    start_time = datetime.now()
    print(f"[{start_time.strftime('%Y-%m-%d %H:%M:%S')}] INFO: Starting Kafka Producer...")

    # --- 1. FETCH FROM API ---
    params = {
        'ids': COINS, 'vs_currencies': CURRENCY,
        'include_market_cap': 'true', 'include_24hr_vol': 'true',
        'include_last_updated_at': 'true'
    }
    
    try:
        print(f"INFO: Fetching data for: {COINS}...")
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Add Metadata
        data['ingestion_metadata'] = {
            "source": "CoinGecko API",
            "ingested_at": datetime.now().isoformat()
        }

        # --- 2. PRODUCE TO KAFKA ---
        print(f"INFO: Sending data to Kafka Topic: {KAFKA_TOPIC}...")
        
        # Convert dictionary to JSON string
        json_payload = json.dumps(data)
        
        # Send to Kafka
        producer.produce(
            KAFKA_TOPIC, 
            value=json_payload, 
            callback=delivery_report
        )

        # Wait for any outstanding messages to be delivered
        producer.flush()
        
        print(f"INFO: Production completed in {(datetime.now() - start_time).total_seconds():.2f} seconds.")

    except Exception as e:
        print(f"❌ FAILED: {e}")
        raise e

if __name__ == "__main__":
    run_kafka_producer()
