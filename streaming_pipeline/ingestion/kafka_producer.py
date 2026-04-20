"""
File        : kafka_producer.py
Location    : streaming_pipeline/ingestion/
Description : Continuously fetches real-time cryptocurrency prices from the
              CoinGecko API and publishes them as JSON messages to a Kafka topic.
              Runs as a long-lived process, producing one message every 60 seconds.

Output      : Kafka topic — crypto_prices

Message Format:
    {
        "bitcoin":  { "usd": float, "usd_market_cap": float, "usd_24h_vol": float },
        "ethereum": { "usd": float, ... },
        "solana":   { "usd": float, ... },
        "ingestion_metadata": { "source": str, "ingested_at": str }
    }

Dependencies:
    - requests
    - confluent-kafka

Environment Variables Required (.env):
    - KAFKA_BROKER   (default: 127.0.0.1:9092)
    - KAFKA_TOPIC    (default: crypto_prices)

Usage:
    python kafka_producer.py
    Press Ctrl+C to stop.

Warning:
    Requires a running Kafka broker on KAFKA_BROKER before starting.
"""


import os
import json
import socket
import time
import requests
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()


# ── Configuration ─────────────────────────────────────────────────────────────
COINS             = "bitcoin,ethereum,solana"
CURRENCY          = "usd"
BASE_URL          = "https://api.coingecko.com/api/v3/simple/price"
KAFKA_TOPIC       = os.getenv("KAFKA_TOPIC", "crypto_prices")
KAFKA_BROKER      = os.getenv("KAFKA_BROKER", "127.0.0.1:9092")
POLL_INTERVAL_SEC = 60


# ── Kafka Producer Config ─────────────────────────────────────────────────────
conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "client.id"        : socket.gethostname(),
}


# ── Delivery Callback ─────────────────────────────────────────────────────────
def delivery_report(err, msg):
    if err is not None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] DELIVERY FAILED : {err}")
    else:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] DELIVERED       : topic={msg.topic()} partition=[{msg.partition()}]")


# ── Producer ──────────────────────────────────────────────────────────────────
def run_kafka_producer():
    print(f"INFO  : Initializing Kafka producer")
    print(f"INFO  : Broker = {KAFKA_BROKER} | Topic = {KAFKA_TOPIC}")
    print(f"INFO  : Poll interval = {POLL_INTERVAL_SEC}s | Press Ctrl+C to stop")
    print("─" * 60)

    producer = Producer(conf)

    params = {
        "ids"                    : COINS,
        "vs_currencies"          : CURRENCY,
        "include_market_cap"     : "true",
        "include_24hr_vol"       : "true",
        "include_last_updated_at": "true",
    }

    try:
        while True:
            try:
                response = requests.get(BASE_URL, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                data["ingestion_metadata"] = {
                    "source"      : "CoinGecko API",
                    "ingested_at" : datetime.now().isoformat(),
                    "coins"       : COINS,
                }

                producer.produce(
                    topic    = KAFKA_TOPIC,
                    key      = "crypto_update",
                    value    = json.dumps(data),
                    callback = delivery_report,
                )
                producer.flush()

            except requests.exceptions.Timeout:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING : API request timed out, retrying next cycle")
            except requests.exceptions.ConnectionError:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING : Could not reach CoinGecko API, retrying next cycle")
            except requests.exceptions.HTTPError as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] WARNING : HTTP error — {e}")
            except Exception as e:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] ERROR   : Unexpected error — {e}")

            time.sleep(POLL_INTERVAL_SEC)

    except KeyboardInterrupt:
        print("\n" + "─" * 60)
        print("INFO  : Producer stopped by user")
        print("─" * 60)


# ── Entry Point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_kafka_producer()