# Crypto Data Lakehouse Pipeline

An end-to-end data engineering project that ingests live cryptocurrency market data for Bitcoin, Ethereum, and Solana through two parallel pipelines — batch and streaming — and stores it in a Medallion Architecture (Bronze → Silver → Gold) using Delta Lake, Kafka, PySpark, Databricks, and AWS S3.

Built this to get hands-on with how real-world data lakehouses are designed — specifically around the challenge of keeping batch and streaming outputs consistent in the same Gold layer.

---

## What this project does

The pipeline pulls price, market cap, and volume data from the CoinGecko API and processes it two ways simultaneously:

- **Batch** — runs hourly via Airflow, fetches data to S3, transforms it through Databricks using PySpark
- **Streaming** — a Kafka producer publishes new prices every 60 seconds, consumed by Spark Structured Streaming jobs

Both pipelines write into the same three-layer Medallion structure (Bronze → Silver → Gold), so the Gold tables always have a unified view regardless of which pipeline wrote it.

---

## Project summary

| Property | Details |
|---|---|
| Coins tracked | Bitcoin, Ethereum, Solana |
| Data source | CoinGecko public API |
| Batch schedule | Hourly |
| Streaming interval | Every 60 seconds |
| Architecture | Medallion (Bronze / Silver / Gold) + Lambda |
| Orchestration | Apache Airflow (Astronomer Astro) |
| Processing | PySpark + Spark Structured Streaming |
| Storage | AWS S3 + Delta Lake |
| Batch compute | Databricks |
| Message broker | Apache Kafka + Zookeeper |

---

## Architecture

![Overall Architecture](docs/architecture/01_overall_architecture.png)

---

## Tech stack

| Layer | Technology |
|---|---|
| Data source | CoinGecko REST API |
| Ingestion (batch) | Python, Boto3 |
| Ingestion (streaming) | Confluent Kafka, kafka-python |
| Message broker | Apache Kafka, Zookeeper |
| Stream processing | Apache Spark Structured Streaming |
| Batch processing | Databricks, PySpark |
| Table format | Delta Lake |
| Storage | AWS S3 |
| Orchestration | Apache Airflow, Astronomer Astro CLI |
| Runtime | Docker, Astro Runtime 11.3.0 |
| Language | Python 3.8+ |

---

## Medallion Architecture

![Medallion Layers](docs/architecture/02_medallion_layers.png)

### Bronze — Raw data
Stores data exactly as received from the source, no transformations applied. If something breaks downstream, raw data is always preserved here.

- Batch: timestamped JSON files on S3 at `s3://crypto-lakehouse-neha/bronze/`
- Streaming: nested Delta table at `data/bronze/crypto_prices_delta`

### Silver — Cleaned and validated
Flattens nested coin objects into structured rows, adds derived fields, and enforces quality rules. A validation gate runs before promotion to Gold.

- One row per coin per ingestion event
- Fields: `coin_id`, `price_usd`, `market_cap`, `volume_24h`, `event_timestamp`, `price_change_flag`, `ingestion_delay_seconds`
- Batch: Databricks Unity Catalog table `workspace.default.silver_crypto_prices`
- Streaming: S3 Delta table `s3a://crypto-lakehouse-neha/silver/crypto_prices_clean`

### Gold — Business ready
Three analytics-ready aggregation tables built from Silver. These are what a BI tool or analyst would actually query.

| Table | Description |
|---|---|
| `gold_latest_snapshot` | Most recent price per coin — always 3 rows |
| `gold_price_performance` | Moving average, volatility, market cap rank |
| `gold_daily_trends` | Daily avg, max, min price and volume per coin |

---

## Repository structure

```
Crypto-Data-lakehouse-pipeline/
│
├── batch_pipeline/                        # Hourly batch pipeline
│   ├── ingestion/
│   │   └── crypto_api_fetch.py            # Fetches API data, uploads to S3
│   ├── medallion/
│   │   ├── bronze/
│   │   │   └── bronze_ingestion_notes.py  # Bronze runs inside Databricks session
│   │   ├── silver/
│   │   │   ├── silver_transformations.py  # Bronze → Silver on Databricks
│   │   │   └── silver_validation.py       # Silver quality gate
│   │   └── gold/
│   │       ├── gold_transformations.py    # Silver → Gold on Databricks
│   │       ├── gold_checks.py             # SQL inspection queries
│   │       └── gold_validations.py        # Gold business logic validation
│   └── README.md
│
├── streaming_pipeline/                    # Real-time streaming pipeline (Astro project)
│   ├── dags/
│   │   └── airflow_dag_streaming.py       # Streaming orchestration DAG
│   ├── include/
│   │   └── medallion/
│   │       ├── silver/
│   │       │   ├── silver_transformations.py
│   │       │   └── silver_validation.py
│   │       └── gold/
│   │           ├── gold_transformations.py
│   │           ├── gold_checks.py
│   │           └── gold_validations.py
│   ├── ingestion/
│   │   ├── kafka_producer.py              # Publishes to Kafka every 60s
│   │   └── Stream_Ingestion_Bronze.py     # Spark consumer → Bronze Delta
│   ├── Dockerfile
│   ├── requirements.txt
│   └── README.md
│
├── dags/
│   └── airflow_dag_batch.py               # Batch orchestration DAG
│
├── config/
│   └── config_template.py
│
├── docs/
│   ├── architecture/                      # Architecture diagrams
│   │   ├── 01_overall_architecture.png
│   │   ├── 02_medallion_layers.png
│   │   ├── 03_streaming_flow.png
│   │   ├── 04_batch_flow.png
│   │   └── 05_airflow_dag.png
│   ├── data_catalog.md                    # All tables, locations, schemas
│   └── data_dictionary.md                 # All field definitions and terms
│
├── tests/
│   ├── batch/
│   ├── streaming/
│   └── dags/
│       └── test_dag_example.py
│
├── .dockerignore
├── .gitignore
├── airflow_settings.yaml
├── docker-compose.yaml                    # Kafka + Zookeeper services
├── Dockerfile                             # Astro Runtime 11.3.0 + Java 11
├── packages.txt
└── requirements.txt
```

---

## Pipeline flows

### Batch pipeline

![Batch Flow](docs/architecture/04_batch_flow.png)

```
CoinGecko API
    → crypto_api_fetch.py  (Python + Boto3)
    → S3 Bronze JSON
    → Databricks Silver transformation
    → Databricks Gold aggregations
    → Airflow DAG triggers jobs hourly (DatabricksRunNowOperator)
```

### Streaming pipeline

![Streaming Flow](docs/architecture/03_streaming_flow.png)

```
CoinGecko API
    → kafka_producer.py  (every 60 seconds)
    → Kafka broker (port 9092)
    → Stream_Ingestion_Bronze.py  (Spark Structured Streaming)
    → Bronze Delta (local)
    → silver_transformations.py  (S3)
    → gold_transformations.py    (S3 — 3 tables)
    → Airflow DAG manages execution and validation
```

### Airflow DAG structure

![Airflow DAG](docs/architecture/05_airflow_dag.png)

**Batch DAG** (`crypto_databricks_pipeline` — `@hourly`):
```
trigger_fetcher_uploader >> trigger_transformations
```

**Streaming DAG** (`crypto_medallion_streaming` — manual trigger):
```
run_silver_stream >> validate_silver_data
run_gold_stream   >> validate_gold_data
```

---

## Data quality

Every medallion layer has a dedicated validation script that acts as a quality gate before data moves to the next layer. The pipeline does not promote data that fails these checks.

| Layer | Pipeline | File | Key checks |
|---|---|---|---|
| Bronze | Batch | `bronze_ingestion_notes.py` | JSON integrity, schema contract, NULL prices, freshness |
| Silver | Batch | `silver_validation.py` | Duplicates, NULL prices, SLA, anomaly detection, price stability |
| Gold | Batch | `gold_validations.py` | Ranking integrity, moving averages, cross-table sync |
| Silver | Streaming | `silver_validation.py` | Coin coverage, freshness, volume, anomaly detection |
| Gold | Streaming | `gold_validations.py` | Snapshot freshness, windowing integrity, analytics completeness |

---

## Setup and running

### Prerequisites

- Python 3.8+
- Docker Desktop
- Astro CLI
- Java JDK 11 (Amazon Corretto recommended)
- AWS account with S3 bucket
- Databricks workspace with Unity Catalog

### Install Astro CLI

```bash
# Windows
winget install -e --id Astronomer.Astro

# Mac
brew install astro
```

### Environment variables

Copy `.env.example` to `.env` and fill in your values:

```bash
cp .env.example .env
```

```env
# Airflow
AIRFLOW_UID=50000
FERNET_KEY=your_generated_fernet_key
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# AWS
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
S3_BUCKET_NAME=your-bucket-name

# Kafka
KAFKA_BROKER=localhost:9092
KAFKA_TOPIC=crypto_prices

# Java
JAVA_HOME=/path/to/jdk11
HADOOP_HOME=/path/to/hadoop
```

Generate a Fernet key:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### Running the batch pipeline

```bash
pip install -r requirements.txt
astro dev start
# Open http://localhost:8080 and enable: crypto_databricks_pipeline
```

### Running the streaming pipeline

```bash
# Start Kafka and Zookeeper
docker-compose up zookeeper kafka -d

# Start Airflow from inside the streaming folder
cd streaming_pipeline
astro dev start

# In a separate terminal — start the Kafka producer
python ingestion/kafka_producer.py

# In another terminal — start the Bronze Spark consumer
python ingestion/Stream_Ingestion_Bronze.py

# Trigger the streaming DAG manually at http://localhost:8080
```

---

## Documentation

| Document | Description |
|---|---|
| [`docs/data_catalog.md`](docs/data_catalog.md) | All tables across both pipelines — locations, formats, schemas, write modes |
| [`docs/data_dictionary.md`](docs/data_dictionary.md) | Every field defined — source, derived, flags, aggregations |
| [`batch_pipeline/README.md`](batch_pipeline/README.md) | Batch pipeline setup, schema, and quality gates |
| [`streaming_pipeline/README.md`](streaming_pipeline/README.md) | Streaming pipeline setup, Kafka config, and DAG details |

---

## What I learned building this

The hardest part of this project wasn't the code — it was Docker. Running Airflow, Kafka, Zookeeper, and Spark simultaneously meant managing multiple containers that all needed to talk to each other, and getting the networking right took a lot of trial and error. Port conflicts, container startup order, and making sure the Kafka broker was reachable from inside the Airflow container were the kinds of issues that don't show up in tutorials.

On Windows specifically, the Hadoop binary setup and JAVA_HOME configuration added another layer — Spark inside Docker would silently fail without the right Java version, and tracking that down took longer than I'd like to admit.

If I were to redo this, I'd spend more time upfront designing the docker-compose networking rather than debugging it reactively.

---

## Project status

| Component | Status |
|---|---|
| Batch ingestion — CoinGecko → S3 | Complete |
| Batch Bronze Delta layer | Complete |
| Batch Silver — Databricks | Complete |
| Batch Gold — 3 Delta tables | Complete |
| Batch Airflow DAG | Complete |
| Streaming Kafka producer | Complete |
| Streaming Bronze Delta | Complete |
| Streaming Silver — S3 | Complete |
| Streaming Gold — 3 Delta tables | Complete |
| Streaming Airflow DAG | Complete |
| Data quality gates (all layers) | Complete |
| Documentation | Complete |
| Run screenshots / execution proof | Coming soon |

---

*Built with Python, PySpark, Delta Lake, Apache Kafka, Apache Airflow, Databricks, and AWS S3.*
