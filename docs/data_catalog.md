# Streaming Pipeline

Real-time ingestion of cryptocurrency market data from the CoinGecko API through Apache Kafka into a Medallion Architecture on AWS S3 using Spark Structured Streaming.

---

## Overview

The streaming pipeline continuously fetches live price data for **Bitcoin, Ethereum, and Solana** every 60 seconds using the CoinGecko public API. Data is published to a Kafka topic, consumed by a Spark Structured Streaming job, and written to Delta Lake tables through Bronze, Silver, and Gold medallion layers. The pipeline runs locally using Astronomer (Astro) for Airflow orchestration and Docker for Kafka infrastructure.

---

## Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow (Astro) | DAG management and task scheduling |
| Message broker | Apache Kafka + Zookeeper | Real-time event streaming |
| Data source | CoinGecko API | Live crypto market data |
| Stream processing | Spark Structured Streaming | Continuous Bronze, Silver, Gold writes |
| Table format | Delta Lake | ACID transactions, schema evolution |
| Storage | AWS S3 | Silver and Gold Delta tables |
| Local storage | Delta Lake (local) | Bronze Delta table |
| Runtime | Docker + Astro Runtime | Local containerised execution |

---

## Architecture — Medallion Layers

### Streaming flow

```
CoinGecko API
      ↓
kafka_producer.py          (every 60 seconds)
      ↓
Kafka broker               (topic: crypto_prices, port: 9092)
      ↓
Stream_Ingestion_Bronze.py (Spark Structured Streaming)
      ↓
Bronze Delta table         (local — data/bronze/crypto_prices_delta)
      ↓
silver_transformations.py  (Spark Structured Streaming → S3)
      ↓
gold_transformations.py    (Spark Structured Streaming → S3)
```

### Bronze — Raw stream ingestion

Spark Structured Streaming reads messages from the Kafka topic and writes raw nested coin data directly to a local Delta table.

- No transformations — raw JSON structure preserved
- Watermark of 10 minutes applied for late data handling
- Checkpoint stored at `checkpoints/bronze_ingestion`

### Silver — Cleaned and validated

Reads the Bronze Delta stream, unpivots nested coin data into flat rows, and writes to S3.

- Unpivots `bitcoin`, `ethereum`, `solana` objects into one row per coin per message using `stack()` expression
- Casts and cleans all fields — price, market cap, volume, timestamps
- Filters NULL prices
- Applies 10-minute watermark on `event_timestamp`
- Written in append mode to `s3a://crypto-lakehouse-neha/silver/crypto_prices_clean`

### Gold — Business ready

Reads Silver stream and produces three Gold Delta tables on S3:

| Table | Window | Description |
|---|---|---|
| `gold/daily_trends` | 24-hour tumbling | Avg, max, min price and volume per coin per day |
| `gold/price_performance` | 5-min sliding (1-min slide) | Moving average and price volatility per coin |
| `gold/latest_snapshot` | foreachBatch upsert | Most recent price per coin — always current |

---

## Folder Structure

```
streaming_pipeline/
├── .astro/
│   ├── config.yaml
│   ├── dag_integrity_exceptions.txt
│   └── test_dag_integrity_default.py
├── dags/
│   ├── .airflowignore
│   └── airflow_dag_streaming.py       # Airflow DAG for streaming orchestration
├── include/
│   └── medallion/
│       ├── silver/
│       │   ├── silver_transformations.py  # Bronze → Silver stream
│       │   └── silver_validation.py       # Silver quality gate
│       └── gold/
│           ├── gold_transformations.py    # Silver → Gold stream
│           ├── gold_checks.py             # Gold table inspection
│           └── gold_validations.py        # Gold business logic validation
├── ingestion/
│   ├── kafka_producer.py              # Publishes API data to Kafka every 60s
│   └── Stream_Ingestion_Bronze.py     # Spark consumer → Bronze Delta
├── plugins/
├── tests/
│   └── dags/
│       └── test_dag_example.py
├── .dockerignore
├── .env
├── .gitignore
├── airflow_settings.yaml
├── Dockerfile
├── packages.txt
├── requirements.txt
└── README.md
```

---

## Setup

### Prerequisites

- Python 3.8+
- Docker Desktop running
- Astro CLI installed
- Java JDK 11 (Amazon Corretto recommended)
- AWS account with S3 bucket: `crypto-lakehouse-neha`

### Install Astro CLI

```bash
# Windows
winget install -e --id Astronomer.Astro

# Mac
brew install astro
```

### Environment variables

Create a `.env` file inside the `streaming_pipeline/` folder:

```env
JAVA_HOME=C:/Program Files/Amazon Corretto/jdk11.0.30_7
HADOOP_HOME=C:/hadoop
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_REGION=us-east-1
S3_BUCKET=s3a://crypto-lakehouse-neha
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=crypto_prices
BRONZE_PATH=data/bronze/crypto_prices_delta
CHECKPOINT_PATH=checkpoints/bronze_ingestion
```

### Running the pipeline

**Step 1 — Start Kafka and Zookeeper**
```bash
docker-compose up zookeeper kafka -d
```

**Step 2 — Start Airflow**
```bash
cd streaming_pipeline
astro dev start
```

**Step 3 — Start the Kafka producer**
```bash
python ingestion/kafka_producer.py
```

**Step 4 — Start the Bronze consumer**
```bash
python ingestion/Stream_Ingestion_Bronze.py
```

**Step 5 — Trigger the Airflow DAG manually**
```
http://localhost:8080
Login: airflow / airflow
Trigger DAG: crypto_medallion_streaming
```

---

## Airflow DAG — Task Dependencies

```
[run_silver_stream]   >>  [validate_silver_data]
[run_gold_stream]     >>  [validate_gold_data]
```

Silver and Gold streams run in parallel. Each validation task runs only after its respective stream job completes.

> **Note:** `schedule_interval=None` — this DAG is triggered manually only. Streaming jobs are long-running processes; Airflow manages their execution and validation sequence.

---

## Data Schema

### Kafka message format

```json
{
  "bitcoin":  { "usd": 65000.0, "usd_market_cap": 1.2e12, "usd_24h_vol": 3.2e10, "last_updated_at": 1713456789 },
  "ethereum": { "usd": 3100.0,  "usd_market_cap": 3.7e11, "usd_24h_vol": 1.5e10, "last_updated_at": 1713456789 },
  "solana":   { "usd": 145.0,   "usd_market_cap": 6.5e10, "usd_24h_vol": 2.1e9,  "last_updated_at": 1713456789 },
  "ingestion_metadata": { "source": "CoinGecko API", "ingested_at": "2024-04-18T12:34:56" }
}
```

### Silver — Flattened fields

| Field | Type | Description |
|---|---|---|
| `coin_id` | string | `bitcoin`, `ethereum`, or `solana` |
| `price_usd` | double | Price in USD |
| `market_cap` | double | Market capitalisation |
| `volume_24h` | double | 24-hour trading volume |
| `event_timestamp` | timestamp | When the price was recorded |
| `ingested_at` | timestamp | When the message was produced |

---

## Data Quality Gates

| Layer | File | Checks |
|---|---|---|
| Silver | `silver_validation.py` | Integrity, coin coverage, freshness, volume, anomaly detection |
| Gold | `gold_validations.py` | Snapshot freshness, windowing integrity, cross-layer sync, analytics completeness |

---

## Pipeline Status

| Component | Status |
|---|---|
| Kafka producer | Complete — publishes every 60s |
| Bronze Delta stream | Complete — local Delta table written |
| Silver transformation stream | Complete — writing to S3 |
| Gold transformation streams | Complete — 3 tables on S3 |
| Airflow DAG | Complete — manual trigger configured |

---

## Architecture Diagram

See [`docs/architecture/03_streaming_flow.png`](../docs/architecture/03_streaming_flow.png) for the full streaming pipeline flow diagram.

---

*Part of the Crypto Data Lakehouse Pipeline — built with Kafka, Spark Structured Streaming, Delta Lake and AWS S3.*

