# Crypto Data Lakehouse Pipeline

An end-to-end data lakehouse project implementing both **Batch** and **Near Real-Time Streaming** pipelines to ingest, transform, validate, and analyze cryptocurrency market data. 

The project follows the **Medallion Architecture (Bronze → Silver → Gold)** on Databricks with data stored as Delta tables in AWS S3 and managed via Unity Catalog Volumes. It tracks 15 major cryptocurrencies (including Bitcoin, Ethereum, Solana, and Ripple) across two ingestion modes: hourly batch snapshots from the CoinGecko API and high-frequency live ticks via Kafka.

<br>

## Architecture Overview

This lakehouse unifies two distinct processing paradigms under a single storage and governance layer:

```
[ Batch Ingestion ] ----> CoinGecko API ----> Databricks Job (Hourly) ---                                                                          +--> [ Medallion Layers ] --> AWS S3 (Delta Lake)
[ Streaming Ingestion ] -> Kafka Producer -> Confluent Cloud Kafka ------/     (Bronze -> Silver -> Gold)
```

Detailed visual breakdowns of the flows can be found in the documentation directory:
* **Overall System Layout:** `docs/architecture/01_overall_architecture.png`
* **Medallion Data Boundaries:** `docs/architecture/02_medallion_layers.png`

<br>
<br>

## Project Structure

The repository is structured to separate the two independent processing pipelines while sharing unified documentation and schema design definitions:

```
Crypto-Data-Lakehouse-Pipeline/
├── .github/workflows/
│   ├── sync_batch.yml               # CI/CD workflow for the batch pipeline
│   └── sync_streaming.yml           # CI/CD workflow for the streaming pipeline
├── batch_pipeline/                  # Hourly batch ETL engine (CoinGecko API -> S3)
│   ├── ingestion/                   # API consumers and raw schema validation
│   ├── Transformations/             # Medallion processing and data quality checks
│   └── README.md                    # Detailed batch deployment guide
├── streaming_pipeline/              # Near real-time engine (Kafka -> Structured Streaming)
│   ├── ingestion/                   # Event simulation and Kafka producers
│   ├── transformations/             # Micro-batch transformations and lag checks
│   └── README.md                    # Detailed streaming deployment guide
├── docs/                            # Unified project documentation
│   ├── architecture/                # System, batch, streaming, and Airflow DAG diagrams
│   ├── data_catalog.md              # Target Delta table definitions and locations
│   └── data_dictionary.md           # Column types, constraints, and descriptions
└── requirements.txt                 # Shared Python dependencies
```

<br>
<br>

## Pipeline Mechanics

### 1. Batch Pipeline (`/batch_pipeline`)
* **Ingestion:** Fetches structured JSON snapshots from the CoinGecko API at regular intervals (`crypto_api_fetch.py`).
* **Processing Flow:** Data paths move sequentially from raw JSON captures down to structured analytical representations (`docs/architecture/04_batch_flow.png`).
* **Orchestration:** Scheduled via Databricks Workflows, running a 7-task linear dependency chain.

### 2. Streaming Pipeline (`/streaming_pipeline`)
* **Ingestion:** A custom mock producer simulates real-time price changes within a $\pm1\%$ variance baseline and streams records to Confluent Cloud Kafka topics at 5-second intervals.
* **Processing Flow:** Structured Streaming jobs process available records using micro-batches (`docs/architecture/03_streaming_flow.png`).
* **Execution Strategy:** Configured with PySpark's `.trigger(availableNow=True)` setting. This provides the architectural benefits of streaming (state management, checkpointing, event-time processing) with the cost efficiency of batch execution profiles.

<br>
<br>

## Core Medallion Design

Both pipelines isolate data transformation phases to guarantee balance between raw auditability and optimized analytical performance:

| Layer | Implementation Details | Purpose |
| :--- | :--- | :--- |
| **Bronze** | Raw data formats (Unflattened API JSON or raw Kafka payloads). | Permanent, immutable audit trail of every source record. |
| **Silver** | Cleaned, deduplicated on `(coin_id, timestamp)`, type-cast, and enhanced with derived features like `price_change_flag` (UP / DOWN / STABLE). | Clean corporate data truth for general-purpose querying. |
| **Gold** | Processed via `MERGE INTO` or `foreachBatch` into three analytical targets:<br>• `latest_snapshot`: Absolute current price per coin.<br>• `price_performance`: Volatility tracking and a 7-point moving average.<br>• `daily_trends`: Aggregated high, low, and average values. | Power-user analytics and operational dashboards. |

<br>
<br>

## Data Quality, Testing & Governance

Data cannot progress to downstream layers unless it passes strict, automated validation thresholds executed as dedicated notebook stages within your Databricks workflows:

* **Ingestion Audits:** Validates data structures against rigid schema definitions, catches corrupt payloads, and tracks ingestion lag metrics.
* **Data Sanity Checks:** Ensures structural parameters are met (e.g., verifying that Bitcoin ranges track reasonably, checking that market cap rankings contain no duplicate positions per timestamp, and asserting that moving averages yield no `null` fields).
* **Cross-Table Verification:** Confirms that individual analytical targets (like a snapshot view vs. a long-term performance table) stay completely synchronized during state transitions.

<br>
<br>

## CI/CD & Deployment Model

Code distribution handles deployment seamlessly without continuous manual file staging:

```
Developer Push → GitHub Repository → GitHub Actions Workflow → Databricks Git Volumes Sync → Jobs API Trigger
```

1. Local adjustments are committed and pushed to the `main` branch on GitHub.
2. Targeted workflows (`sync_batch.yml` or `sync_streaming.yml`) catch code alterations.
3. The automated step targets your designated Databricks workspace Git folder over secure API integrations.
4. Updates roll out immediately, and execution jobs kick off automatically across the lakehouse clusters.

<br>
<br>

## Security, Secret Management & Compute

* **Secret Management:** Critical connection details, database targets, and Confluent Cloud API keys are handled cleanly via Databricks Secrets Scopes (`crypto-pipeline-secrets`). No plain-text variables are exposed in source tracking.
* **Storage Abstraction:** File access routing relies on structured Unity Catalog Volumes instead of direct, legacy DBFS structures for secure, isolated cloud storage paths.
* **Compute Footprint:** Configured to execute entirely over cost-efficient, temporary serverless compute setups or single-node clusters on the Databricks Free / Community Tier ecosystem.

---

### Deep Dives
For specific implementation code, execution run screenshots, or setup rules, check out the dedicated readmes inside the **[Batch Pipeline](./batch_pipeline/README.md)** and **[Streaming Pipeline](./streaming_pipeline/README.md)** directories.
