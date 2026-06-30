#Fetches near real time cryptocurrency data from CoinGecko api 

import json
import time
import random
from confluent_kafka import Producer


EXPECTED_COINS = [
    "bitcoin", "ethereum", "solana", "ripple", "cardano", "dogecoin",
    "polkadot", "polygon", "shiba-inu", "avalanche-2", "chainlink",
    "uniswap", "litecoin", "stellar", "near"
]

# baseline prices used to simulate realistic price movement
BASE_PRICES = {
    "bitcoin": 65000.0, "ethereum": 3500.0, "solana": 140.0, "ripple": 0.50,
    "cardano": 0.45, "dogecoin": 0.12, "polkadot": 6.50, "polygon": 0.65,
    "shiba-inu": 0.000025, "avalanche-2": 35.0, "chainlink": 15.0,
    "uniswap": 7.50, "litecoin": 80.0, "stellar": 0.11, "near": 5.50
}


RUN_DURATION_SECONDS = 300


def read_config():
    try:
        from databricks.sdk.runtime import dbutils
        bootstrap_server = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="confluent_bootstrap_server")
        api_key = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="confluent_api_key")
        api_secret = dbutils.secrets.get(scope="crypto-pipeline-secrets", key="confluent_api_secret")

        config = {
            "bootstrap.servers": bootstrap_server,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
            "session.timeout.ms": "45000",
        }
        return config

    except Exception:
        print("dbutils not available, falling back to local client.properties")
        config = {}
        with open("client.properties") as fh:
            for line in fh:
                line = line.strip()
                if len(line) != 0 and line[0] != "#":
                    parameter, value = line.strip().split('=', 1)
                    config[parameter] = value.strip()
        return config


def delivery_report(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        coin_key = msg.key().decode('utf-8') if msg.key() else "unknown"
        print(f"sent — {coin_key:<12} partition: {msg.partition()}")


def produce_crypto_stream(topic, config):
    producer = Producer(config)

    start_time = time.time()

    try:
        while time.time() - start_time < RUN_DURATION_SECONDS:
            current_epoch = int(time.time() * 1000)

            for coin in EXPECTED_COINS:
                base_p = BASE_PRICES.get(coin, 10.0)
                simulated_price = round(base_p * (1 + random.uniform(-0.01, 0.01)), 6)
                simulated_volume = round(random.uniform(10.0, 5000.0), 3)

                crypto_tick = {
                    "symbol": coin,
                    "price": simulated_price,
                    "volume": simulated_volume,
                    "timestamp": current_epoch
                }

                producer.produce(
                    topic=topic,
                    key=coin.encode('utf-8'),
                    value=json.dumps(crypto_tick).encode('utf-8'),
                    callback=delivery_report
                )

            producer.flush()
            print(f"batch sent at {current_epoch}, sleeping 5s\n")
            time.sleep(5)

    except KeyboardInterrupt:
        print("\nstopped by user")

    finally:
        producer.flush()
        print("producer finished")


def main():
    config = read_config()
    topic = "crypto_market_ticks"
    produce_crypto_stream(topic, config)


if __name__ == "__main__":
    main()