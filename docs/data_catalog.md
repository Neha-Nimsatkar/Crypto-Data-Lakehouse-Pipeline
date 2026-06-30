# Data Catalog

This catalog serves as the definitive structural inventory and architectural registry for all data assets managed within the Crypto Data Lakehouse Pipeline. It defines where data assets live, their physical formats, update strategies, and infrastructure constraints across both localized environments and cloud-backed storage tiers.

---

## Table of Contents
- [Lakehouse Asset Blueprint](#lakehouse-asset-blueprint)
- [Bronze Layer Specifications](#bronze-layer-specifications)
- [Silver Layer Specifications](#silver-layer-specifications)
- [Gold Layer Specifications](#gold-layer-specifications)
- [Infrastructure & Connection Parameters](#infrastructure--connection-parameters)

---

## Lakehouse Asset Blueprint

The pipeline isolates processing layers using a Medallion Architecture pattern, moving data from raw, nested event streams up to highly optimized business intelligence surfaces.

```
[ Ingestion Source ] ──► Bronze Layer (Local: data/bronze/)
                               │
                               ▼ (Flattening & Schema Enforcement)
[ Cleaned & Cast ]   ──► Silver Layer (Cloud: s3a://crypto-lakehouse-neha/silver/)
                               │
                               ▼ (Windowing & State Aggregations)
[ Analytical Views ] ──► Gold Layer   (Cloud: s3a://crypto-lakehouse-neha/gold/)
```

---

## Bronze Layer Specifications

### Table Reference: `crypto_prices_delta`
* **Physical Path:** `data/bronze/crypto_prices_delta`
* **Storage Environment:** Local File System (Isolated inside Docker containers via Astro Runtime)
* **Data Format:** Delta Lake over Raw JSON Payloads
* **Ingestion Mechanics:**
    * Consumed continuously via a dedicated Spark Structured Streaming task parsing live events directly out of Apache Kafka.
    * Maintains a strict **10-minute watermark** to manage late-arriving records safely.
    * Tracks lineage state via local checkpointing directories at `checkpoints/bronze_ingestion`.
* **Business Purpose:** Acts as the permanent, immutable ledger of the raw API output payload. No transformations are applied here.

---

## Silver Layer Specifications

### Table Reference: `crypto_prices_clean`
* **Physical Path:** `s3a://crypto-lakehouse-neha/silver/crypto_prices_clean`
* **Storage Environment:** Cloud Storage (AWS S3 Bucket: `crypto-lakehouse-neha`)
* **Data Format:** Optimized Delta Lake Table Format
* **Transformation Logic:**
    * Utilizes a high-performance Spark SQL **`stack(3, ...)` expression** to dynamically unpivot nested coin structures (`bitcoin`, `ethereum`, `solana`) from a single Kafka packet into independent transactional row entries.
    * Filters out payload anomalies where spot prices evaluate to `NULL`.
    * Derives tracking metrics like `ingestion_delay_seconds` (`ingested_at` minus `event_timestamp`) to actively measure pipeline line-lag.
* **Update Frequency:** Spark Structured Streaming executing in continuous `Append` mode, coordinated by Airflow task tracking.

---

## Gold Layer Specifications

Analytical data layers optimized for consumption by reporting tools and data quality inspection tasks. All gold assets reside securely in cloud Parquet layouts on AWS S3.

### 1. `gold/daily_trends`
* **Physical Path:** `s3a://crypto-lakehouse-neha/gold/daily_trends`
* **Window Model:** **Tumbling Window (Fixed, non-overlapping 24-hour periods)**
* **Aggregation Targets:** Evaluates the `daily_avg_price`, `daily_max_price`, `daily_min_price`, and `daily_avg_volume` grouped per asset token per calendar day.

### 2. `gold/price_performance`
* **Physical Path:** `s3a://crypto-lakehouse-neha/gold/price_performance`
* **Window Model:** **Sliding Window (5-minute window duration, advancing on a 1-minute slide interval)**
* **Aggregation Targets:** Computes real-time rolling metrics including a moving average (`moving_avg_price`) and dynamic price volatility tracking (rolling standard deviation).

### 3. `gold/latest_snapshot`
* **Physical Path:** `s3a://crypto-lakehouse-neha/gold/latest_snapshot`
* **Execution Strategy:** Continuous state evaluation managed via a PySpark **`foreachBatch`** output stream loop.
* **Upsert Logic:** Runs an atomic conditional `MERGE INTO` operation on every micro-batch iteration. This enforces a strict partition pattern that ensures the table always holds exactly **one row per coin** containing its absolute latest spot valuation.

---

## Infrastructure & Connection Parameters

To guarantee pipeline consistency and block runtime compilation failures, all storage paths map back to these verified network configurations:

| Parameter Key | Runtime System Configuration | Purpose |
| :--- | :--- | :--- |
| `KAFKA_BROKER` | `localhost:9092` | Localized ingestion broker port mapping. |
| `KAFKA_TOPIC` | `crypto_prices` | Main event log topic containing incoming API ticks. |
| `S3_BUCKET` | `s3a://crypto-lakehouse-neha` | Root cloud target URI passing through Hadoop file connectors. |
| `AWS_REGION` | `us-east-1` | Cloud availability boundary hosting analytics tables. |

---
*Last updated: April 2026 — Crypto Data Lakehouse Pipeline Catalog Reference*
