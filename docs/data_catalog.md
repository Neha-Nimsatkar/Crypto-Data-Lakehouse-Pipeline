# Data Catalog

A structural reference for all data assets in the Crypto Data Lakehouse Pipeline. Covers where data lives, what format it's stored in, how it gets updated, and what tools manage it — across both the batch and streaming pipelines.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Batch Pipeline Assets](#batch-pipeline-assets)
- [Streaming Pipeline Assets](#streaming-pipeline-assets)
- [Infrastructure Reference](#infrastructure-reference)

---

## Architecture Overview

Both pipelines follow a Medallion Architecture — Bronze (raw), Silver (cleaned), Gold (analytical) — but differ in how data enters the system and where it's stored.

```
Batch Pipeline
──────────────
CoinGecko API → Bronze (S3 JSON) → Silver (S3 Delta) → Gold (S3 Delta)
Orchestrated via Databricks Workflows, triggered by GitHub Actions

Streaming Pipeline
──────────────────
Producer (simulated ticks) → Confluent Cloud Kafka → Bronze (Delta table)
→ Silver (Delta table) → Gold (S3 Delta)
Orchestrated via Databricks Workflows, triggered by GitHub Actions
```

---

## Batch Pipeline Assets

### Bronze Layer

| Property | Value |
|---|---|
| Physical path | `s3://crypto-lakehouse-nehaa/bronze/` |
| Format | Raw JSON files, one file per pipeline run |
| Table registration | Not registered in Unity Catalog — raw files only |
| Update strategy | Full overwrite per run — each file is a timestamped snapshot |
| Orchestration | GitHub Actions → Databricks Workflow task `01_Ingestion` |

Raw JSON snapshots from CoinGecko API. One file per run, named `batch_<timestamp>.json`. Includes an `ingestion_metadata` block with source, timestamp, and coin list. No transformations applied — stored exactly as received from the API.

---

### Silver Layer

| Property | Value |
|---|---|
| Physical path | `s3://crypto-lakehouse-nehaa/silver/crypto_prices` |
| Format | Delta Lake |
| Table registration | `workspace.default.silver_crypto_prices` |
| Partition column | `date` |
| Update strategy | Overwrite per run with `mergeSchema: true` |
| Orchestration | Databricks Workflow task `03_Bronze_To_Silver` |

Flattened and cleaned version of the bronze JSON. The nested coin structure (`bitcoin`, `ethereum`, etc.) is unpivoted using `F.explode(F.create_map(...))` into one row per coin per run. Prices are cast to proper types, duplicates removed via Delta `MERGE INTO` logic, and each row gets a `price_change_flag` (UP / DOWN / STABLE) computed using a lag window function.

---

### Gold Layer

All three gold tables live under `s3://crypto-lakehouse-nehaa/gold/` and are registered in Unity Catalog under `workspace.default`.

#### `gold_latest_snapshot`

| Property | Value |
|---|---|
| Physical path | `s3://crypto-lakehouse-nehaa/gold/latest_snapshot/` |
| Table | `workspace.default.gold_latest_snapshot` |
| Update strategy | Overwrite — one row per coin, always the latest |
| Window type | Row-based — `ROW_NUMBER()` partitioned by `coin_id`, ordered by `event_timestamp DESC` |

#### `gold_price_performance`

| Property | Value |
|---|---|
| Physical path | `s3://crypto-lakehouse-nehaa/gold/price_performance/` |
| Table | `workspace.default.gold_price_performance` |
| Partition column | `date` |
| Update strategy | Overwrite per run |
| Window type | Row-based — 7-row preceding window partitioned by `coin_id` |

Contains moving average, rolling standard deviation (volatility), and market cap rank per coin per timestamp. Market cap rank computed using `RANK()` partitioned by `event_timestamp`, ordered by `market_cap` descending.

#### `gold_daily_trends`

| Property | Value |
|---|---|
| Physical path | `s3://crypto-lakehouse-nehaa/gold/daily_trends/` |
| Table | `workspace.default.gold_daily_trends` |
| Partition column | `date` |
| Update strategy | Overwrite per run |
| Aggregation | `groupBy("date", "coin_id")` — daily average, max, min price and volume |

---

## Streaming Pipeline Assets

### Bronze Layer

| Property | Value |
|---|---|
| Table | `workspace.default.crypto_bronze_table` |
| Format | Delta Lake (managed by Databricks Unity Catalog) |
| Physical storage | Managed by Databricks — no explicit S3 path |
| Update strategy | Append — `trigger(availableNow=True)` processes all available Kafka messages |
| Checkpoint | `/Volumes/workspace/default/crypto_silver_volume/checkpoints/bronze_table/` |

Raw tick events consumed from Confluent Cloud Kafka topic `crypto_market_ticks`. Each record has four fields: `symbol`, `price`, `volume`, `timestamp` (epoch milliseconds). Checkpoint is cleared on every run to avoid stale offset conflicts.

---

### Silver Layer

| Property | Value |
|---|---|
| Table | `workspace.default.silver_crypto_prices` |
| Format | Delta Lake (managed by Databricks Unity Catalog) |
| Physical storage | Managed by Databricks — no explicit S3 path |
| Update strategy | Append — `trigger(availableNow=True)` |
| Checkpoint | `/Volumes/workspace/default/crypto_silver_volume/checkpoints/silver_stream_pipeline_v4/` |

Cleaned and enriched version of the bronze stream. Filters out null prices, negative prices, and Bitcoin prices outside a $10,000–$250,000 sanity range. Derives `event_timestamp` from the raw epoch millisecond `timestamp` field. Removes duplicates on `(coin_id, timestamp)`. Computes `ingestion_delay_seconds` as the difference between `ingested_at` and the event's epoch time.

---

### Gold Layer

All three streaming gold tables write to `s3://crypto-lakehouse-nehaa/gold_stream/` and are registered in Unity Catalog under `workspace.default`.

Processed via `foreachBatch` — each micro-batch is handled as a static DataFrame and written independently.

#### `gold_stream_latest_snapshot`

| Property | Value |
|---|---|
| Physical path | `s3://crypto-lakehouse-nehaa/gold_stream/latest_snapshot/` |
| Table | `workspace.default.gold_stream_latest_snapshot` |
| Update strategy | Overwrite per micro-batch — always holds the latest price per coin |
| Window type | Row-based — `ROW_NUMBER()` partitioned by `coin_id`, ordered by `event_timestamp DESC` |

#### `gold_stream_price_performance`

| Property | Value |
|---|---|
| Physical path | `s3://crypto-lakehouse-nehaa/gold_stream/price_performance/` |
| Table | `workspace.default.gold_stream_price_performance` |
| Partition column | `date` |
| Update strategy | Append per micro-batch |
| Window type | Row-based — 7-row preceding window partitioned by `coin_id` |

Contains moving average, rolling standard deviation (volatility), and rank per coin per timestamp. Rank computed using `RANK()` partitioned by `event_timestamp`, ordered by `price_usd` descending (note: batch pipeline ranks by `market_cap`; streaming ranks by `price_usd` since market cap data is not available from the simulated producer).

#### `gold_stream_daily_trends`

| Property | Value |
|---|---|
| Physical path | `s3://crypto-lakehouse-nehaa/gold_stream/daily_trends/` |
| Table | `workspace.default.gold_stream_daily_trends` |
| Partition column | `date` |
| Update strategy | Append per micro-batch |
| Aggregation | `groupBy("date", "coin_id")` — daily average, max, min price and volume |

---

## Infrastructure Reference

| Parameter | Value | Purpose |
|---|---|---|
| S3 bucket | `crypto-lakehouse-nehaa` | Primary cloud storage for all pipeline outputs |
| AWS region | `ap-south-1` | Mumbai region hosting the S3 bucket |
| Kafka cluster | Confluent Cloud (`pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092`) | Managed Kafka for streaming ingestion |
| Kafka topic | `crypto_market_ticks` | Main event stream for streaming pipeline |
| Databricks catalog | `workspace` | Unity Catalog root for all registered tables |
| Databricks schema | `default` | Schema under which all tables are registered |
| Secrets scope | `crypto-pipeline-secrets` | Databricks secrets scope holding AWS and Confluent credentials |
| Checkpoint volume | `/Volumes/workspace/default/crypto_silver_volume/` | Databricks volume hosting all streaming checkpoints |

---
