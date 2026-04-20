# Batch Pipeline

Hourly ingestion of cryptocurrency market data from the CoinGecko API into a Medallion Architecture on AWS S3 and Databricks.

---

## Overview

The batch pipeline fetches real-time price data for **Bitcoin, Ethereum, and Solana** every hour using the CoinGecko public API. Raw data is uploaded to AWS S3 and progressively refined through Bronze, Silver, and Gold layers following the Medallion Architecture pattern. All transformations run on Databricks using PySpark and Delta Lake. The pipeline is orchestrated by Apache Airflow running on Astronomer (Astro).

---

## Tech Stack

| Component | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow (Astro) | Hourly DAG scheduling |
| Data source | CoinGecko API | Real-time crypto market data |
| Ingestion | Python + Boto3 | Fetch and upload raw JSON to S3 |
| Storage | AWS S3 | Raw Bronze layer data lake |
| Processing | Databricks + PySpark | Silver and Gold transformations |
| Table format | Delta Lake | ACID transactions, time travel |
| Validation | PySpark | Quality checks at each layer |

---

## Architecture — Medallion Layers

### Bronze — Raw ingestion

Stores raw, unmodified JSON responses from the CoinGecko API. Each file is timestamped and stored at:

```
s3://crypto-lakehouse-neha/bronze/batch_YYYYMMDD_HHMMSS.json
```

- No transformations applied — data stored exactly as received
- Ingestion metadata (source, timestamp) appended to each record
- Quality gate validates JSON integrity, schema contract, and NULL prices

### Silver — Cleaned and validated

Reads Bronze JSON files, flattens the nested structure, and applies transformations:

- Flattens nested coin objects into structured rows (one row per coin per timestamp)
- Adds `price_change_flag` to indicate directional price movement (UP / DOWN / STABLE)
- Computes `ingestion_delay_seconds` for lineage tracking
- Partitioned by `date` for optimised query performance
- Written as Delta Lake table: `workspace.default.silver_crypto_prices`

### Gold — Business ready

Produces three analytics-ready Delta tables from Silver data:

| Table | Description |
|---|---|
| `gold_latest_snapshot` | Most recent price, market cap and volume per coin |
| `gold_price_performance` | 7-period moving average, volatility and market cap rank |
| `gold_daily_trends` | Daily average, max and min price per coin |

---

## Folder Structure

```
batch_pipeline/
├── ingestion/
│   └── crypto_api_fetch.py          # Fetches API data, uploads to S3
├── medallion/
│   ├── bronze/
│   │   └── bronze_ingestion_notes.py  # Bronze runs inside Databricks session
│   ├── silver/
│   │   ├── silver_transformations.py  # Bronze → Silver transformation
│   │   └── silver_validation.py       # Silver quality gate
│   └── gold/
│       ├── gold_transformations.py    # Silver → Gold transformation
│       ├── gold_checks.py             # SQL inspection queries
│       └── gold_validations.py        # Gold business logic validation
└── README.md
```

---

## Setup

### Prerequisites

- Python 3.8+
- AWS account with S3 bucket: `crypto-lakehouse-neha`
- Databricks workspace with Unity Catalog enabled
- Astro CLI installed

### Environment variables

Create a `.env` file in the project root:

```env
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_REGION=us-east-1
S3_BUCKET_NAME=crypto-lakehouse-neha
```

### Running the pipeline

**Step 1 — Install dependencies**
```bash
pip install -r requirements.txt
```

**Step 2 — Start Airflow**
```bash
astro dev start
```

**Step 3 — Open Airflow UI**
```
http://localhost:8080
Login: airflow / airflow
Enable DAG: crypto_databricks_pipeline
```

The DAG runs automatically every hour. You can also trigger it manually from the Airflow UI.

---

## Data Schema

### Bronze — Raw JSON fields

| Field | Type | Description |
|---|---|---|
| `bitcoin.usd` | float | Bitcoin price in USD |
| `bitcoin.usd_market_cap` | float | Bitcoin market capitalisation |
| `bitcoin.usd_24h_vol` | float | Bitcoin 24-hour trading volume |
| `ethereum.usd` | float | Ethereum price in USD |
| `solana.usd` | float | Solana price in USD |
| `ingestion_metadata.source` | string | Always `CoinGecko API` |
| `ingestion_metadata.ingested_at` | timestamp | UTC timestamp of ingestion |

### Silver — Transformed fields

| Field | Type | Description |
|---|---|---|
| `coin_id` | string | `bitcoin`, `ethereum`, or `solana` |
| `price_usd` | float | Price in USD |
| `market_cap` | float | Market capitalisation in USD |
| `volume_24h` | float | 24-hour trading volume |
| `event_timestamp` | timestamp | When the price was recorded |
| `price_change_flag` | string | `UP`, `DOWN`, or `STABLE` |
| `ingestion_delay_seconds` | float | Delay between event and ingestion |
| `date` | date | Partition column |

---

## Data Quality Gates

Each layer has a dedicated validation script acting as a quality gate before data is promoted to the next layer.

| Layer | File | Checks |
|---|---|---|
| Bronze | `bronze_ingestion_notes.py` | JSON integrity, schema contract, NULL prices, freshness |
| Silver | `silver_validation.py` | Duplicates, NULL prices, SLA freshness, anomaly detection, price stability |
| Gold | `gold_validations.py` | Ranking integrity, moving average completeness, cross-table sync |

---

## Pipeline Status

| Component | Status |
|---|---|
| CoinGecko API ingestion | Complete |
| Bronze Delta layer | Complete — Delta files verified in S3 |
| Silver transformations | Complete — running on Databricks |
| Gold aggregations | Complete — 3 tables produced |
| Airflow DAG | Complete — hourly schedule configured |

---

## Architecture Diagram

See [`docs/architecture/04_batch_flow.png`](../docs/architecture/04_batch_flow.png) for the full batch pipeline flow diagram.

---

*Part of the Crypto Data Lakehouse Pipeline — built with PySpark, Delta Lake, Apache Airflow and AWS S3.*
