import json
import time
import random
from confluent_kafka import Producer

# ── Expected Pipeline 15 Coins Configurations ───────────────────────────────
EXPECTED_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano", "dogecoin", 
    "polkadot", "polygon", "shiba-inu", "avalanche-2", "chainlink", 
    "uniswap", "litecoin", "stellar", "near"
]

# Baseline standard baseline rates for random simulation bounds
BASE_PRICES = {
    "bitcoin": 65000.0, "ethereum": 3500.0, "solana": 140.0, "ripple": 0.50,
    "cardano": 0.45, "dogecoin": 0.12, "polkadot": 6.50, "polygon": 0.65,
    "shiba-inu": 0.000025, "avalanche-2": 35.0, "chainlink": 15.0,
    "uniswap": 7.50, "litecoin": 80.0, "stellar": 0.11, "near": 5.50
}

def read_config():
    # Reads the client configuration from client.properties or client file wrapper
    config = {}
    try:
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
    except FileNotFoundError:
        with open("client") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
    return config

def delivery_report(err, msg):
    if err is not None:
        print(f" Message delivery failed: {err}")
    else:
        # Decoded message key for clear logs tracking
        coin_key = msg.key().decode('utf-8') if msg.key() else "Unknown"
        print(f" Live Tick Sent -> Asset: {coin_key:<12} | Partition: [{msg.partition()}]")

def produce_crypto_stream(topic, config):
    # Creates a new producer instance mapping properties config definitions
    producer = Producer(config)
    
    print(f" Streaming Live Multi-Coin Crypto Data to Confluent Cloud Topic: {topic}...")
    print(f" Active Assets Matrix: Tracked {len(EXPECTED_COINS)} coins dynamically.\n")
    print("Press Ctrl+C to stop streaming anytime.\n")
    
    try:
        while True:
            current_epoch = int(time.time() * 1000)
            
            # Loop through all 15 assets to generate dynamic ticks in a single window iteration
            for coin in EXPECTED_COINS:
                # Add minor volatility flunctuations (-1% to +1%) to match realistic streaming curves
                base_p = BASE_PRICES.get(coin, 10.0)
                simulated_price = round(base_p * (1 + random.uniform(-0.01, 0.01)), 6)
                simulated_volume = round(random.uniform(10.0, 5000.0), 3)
                
                crypto_tick = {
                    "symbol": coin,  # Strict string contract matching Spark validation checks
                    "price": simulated_price,
                    "volume": simulated_volume,
                    "timestamp": current_epoch
                }
                
                # Send message utilizing asset name as unique Kafka partitioning distribution key
                producer.produce(
                    topic=topic, 
                    key=coin.encode('utf-8'),
                    value=json.dumps(crypto_tick).encode('utf-8'), 
                    callback=delivery_report
                )
            
            # Flush pipeline queue buffer immediately per iteration drop
            producer.flush()
            print(f" Iteration batch complete at stamp {current_epoch}. Sleeping for 5 seconds...\n")
            
            # 5 seconds pause between data injection bursts
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\n Stream execution paused gracefully by user.")

def main():
    config = read_config()
    topic = "crypto_market_ticks"
    produce_crypto_stream(topic, config)

if __name__ == "__main__":
    main()
