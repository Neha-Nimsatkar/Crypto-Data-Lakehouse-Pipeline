# Data Dictionary

Field-level definitions for every column, metric, flag, and derived value used across the Crypto Data Lakehouse Pipeline. Where batch and streaming pipelines differ in field names or derivation logic, both are noted explicitly.

---

## Table of Contents

- [Source Fields — Batch](#source-fields--batch)
- [Source Fields — Streaming](#source-fields--streaming)
- [Derived Fields](#derived-fields)
- [Flag Fields](#flag-fields)
- [Aggregated Metrics](#aggregated-metrics)
- [Metadata Fields](#metadata-fields)
- [Technical Terms](#technical-terms)

---

## Source Fields — Batch

Fields that come directly from the CoinGecko API response, stored as-is in the bronze JSON files.

| Field | Type | Description |
|---|---|---|
| `{coin}.usd` | double | Spot price of the coin in USD at the time of the API call. Nested under the coin's key (e.g., `bitcoin.usd`). |
| `{coin}.usd_market_cap` | double | Total market capitalisation in USD — price multiplied by circulating supply. |
| `{coin}.usd_24h_vol` | double | Total trading volume across all exchanges in the last 24 hours, in USD. |
| `{coin}.last_updated_at` | long | Unix epoch timestamp (seconds) of the last price update at the exchange layer. |
| `ingestion_metadata.source` | string | Hardcoded to `"CoinGecko API"` — identifies the origin of the batch. |
| `ingestion_metadata.ingested_at` | string | ISO 8601 UTC timestamp set by the ingestion script at the moment the API was called. |
| `ingestion_metadata.coins` | string | Comma-separated list of coin IDs requested in that batch run. |
| `ingestion_metadata.currency` | string | Currency used for pricing — always `"usd"` in this pipeline. |

---

## Source Fields — Streaming

Fields that come directly from the simulated producer via Confluent Cloud Kafka. Each Kafka message contains one tick for one coin.

| Field | Type | Description |
|---|---|---|
| `symbol` | string | Coin identifier (e.g., `bitcoin`, `ethereum`). Used as the Kafka message key for partition routing. |
| `price` | double | Simulated spot price in USD — randomised within ±1% of a CoinGecko-sourced baseline value. |
| `volume` | double | Simulated 24-hour trading volume — random value between 10.0 and 5000.0. |
| `timestamp` | long | Unix epoch timestamp in **milliseconds** set by the producer at the moment the tick was generated. |

---

## Derived Fields

Fields computed during Silver transformation from raw source data. Most fields exist in both pipelines but are derived differently due to different source schemas.

| Field | Type | Pipeline | Derived from | Description |
|---|---|---|---|---|
| `coin_id` | string | Both | JSON key (batch) / `symbol` field (streaming) | Standardised coin identifier. In batch, extracted when nested coin columns are unpivoted. In streaming, renamed from `symbol`. |
| `price_usd` | double | Both | `{coin}.usd` (batch) / `price` (streaming) | Coin spot price in USD, cast to double. |
| `market_cap` | double | Batch only | `{coin}.usd_market_cap` | Market capitalisation in USD. Not available in streaming since the simulated producer doesn't include it. |
| `volume_24h` | double | Both | `{coin}.usd_24h_vol` (batch) / `volume` (streaming) | 24-hour trading volume in USD. |
| `event_timestamp` | timestamp | Both | `{coin}.last_updated_at` (batch) / `timestamp` (streaming) | When the price event occurred at the source. Batch: converted from Unix seconds. Streaming: converted from Unix milliseconds using `from_unixtime(timestamp / 1000)`. |
| `date` | date | Both | `event_timestamp` | Date portion of `event_timestamp`. Used as a physical partition column for S3 storage efficiency. |
| `hour` | int | Both | `event_timestamp` | Hour of day extracted from `event_timestamp`. |
| `ingested_at` | timestamp | Both | `ingestion_metadata.ingested_at` (batch) / `current_timestamp()` (streaming) | When the data was ingested into the pipeline. |
| `ingestion_delay_seconds` | long | Both | `unix_timestamp(ingested_at) - api_last_updated_at` (batch) / `ingested_at - (timestamp / 1000)` (streaming) | Seconds between when the price was recorded at source and when it landed in the pipeline. Negative values indicate clock sync issues and are filtered out before gold. |
| `load_timestamp` | timestamp | Both | `current_timestamp()` | When the record was written to the Silver Delta table. Used for lineage tracking. |

---

## Flag Fields

Categorical fields that indicate data quality or business conditions.

| Field | Type | Pipeline | Possible values | Description |
|---|---|---|---|---|
| `price_change_flag` | string | Batch only | `UP`, `DOWN`, `STABLE` | Direction of price movement compared to the previous record for the same coin. Computed using a `LAG()` window function partitioned by `coin_id`, ordered by `event_timestamp`. |

**Logic for `price_change_flag`:**

```
current_price > previous_price  →  UP
current_price < previous_price  →  DOWN
current_price = previous_price  →  STABLE
previous_price IS NULL          →  STABLE  (first record for that coin)
```

This field exists in batch silver only. The streaming pipeline does not compute it since the simulated tick stream doesn't have stable enough price history within a single micro-batch to make it meaningful.

---

## Aggregated Metrics

Fields produced in the Gold layer transformations.

| Field | Type | Table | Pipeline | Description |
|---|---|---|---|---|
| `moving_avg_price` | double | `gold_price_performance` / `gold_stream_price_performance` | Both | Rolling average of `price_usd` over a 7-row preceding window, partitioned by `coin_id`. Rounded to 4 decimal places. |
| `price_volatility` | double | `gold_price_performance` / `gold_stream_price_performance` | Both | Rolling standard deviation of `price_usd` over the same 7-row window. Higher values indicate more unstable prices. NULL when only one record exists in the window — coalesced to `0.0`. Rounded to 6 decimal places. |
| `market_cap_rank` | int | `gold_price_performance` / `gold_stream_price_performance` | Both | Rank of each coin per timestamp. Batch: ranked by `market_cap` descending. Streaming: ranked by `price_usd` descending (market cap not available from simulated producer). `1` = highest value. |
| `daily_avg_price` | double | `gold_daily_trends` / `gold_stream_daily_trends` | Both | Average `price_usd` for all records within that calendar day per coin. Rounded to 4 decimal places. |
| `daily_max_price` | double | `gold_daily_trends` / `gold_stream_daily_trends` | Both | Maximum `price_usd` recorded within that calendar day per coin. Rounded to 4 decimal places. |
| `daily_min_price` | double | `gold_daily_trends` / `gold_stream_daily_trends` | Both | Minimum `price_usd` recorded within that calendar day per coin. Rounded to 4 decimal places. |
| `daily_avg_volume` | double | `gold_daily_trends` / `gold_stream_daily_trends` | Both | Average `volume_24h` for all records within that calendar day per coin. Rounded to 2 decimal places. |
| `record_count` | long | `gold_daily_trends` / `gold_stream_daily_trends` | Both | Number of Silver records that contributed to the daily aggregate for that coin and day. |

---

## Metadata Fields

Fields that track lineage and pipeline execution context.

| Field | Type | Layer | Description |
|---|---|---|---|
| `ingestion_metadata` | struct | Bronze (batch) | Nested struct attached by the ingestion script. Contains `source`, `ingested_at`, `coins`, and `currency`. |
| `ingested_at` | timestamp | Silver (both) | Parsed, queryable timestamp of when data entered the pipeline. Batch: parsed from `ingestion_metadata.ingested_at` ISO string. Streaming: set to `current_timestamp()` at write time. |
| `load_timestamp` | timestamp | Silver + Gold (both) | Timestamp of when the record was written to the current layer's Delta table. Used to track processing lag across layers. |

---

## Technical Terms

| Term | Definition |
|---|---|
| **Medallion Architecture** | A data design pattern that organises data into three progressive layers — Bronze (raw), Silver (cleaned), Gold (aggregated) — each increasing in quality and business value. |
| **Delta Lake** | An open-source storage layer that adds ACID transactions, schema enforcement, and upsert support on top of Parquet files stored in S3 or managed storage. |
| **Structured Streaming** | Apache Spark's engine for processing continuous data streams using the same DataFrame API as batch processing. Used for all streaming pipeline tasks. |
| **trigger(availableNow=True)** | A Spark Structured Streaming trigger mode that processes all currently available data in one run and then stops. Used in this pipeline for all streaming tasks — making it near real-time rather than continuous. |
| **foreachBatch** | A Spark Structured Streaming output mode that passes each micro-batch as a static DataFrame to a custom function. Used in the gold transformation to write three separate tables from one stream. |
| **Checkpoint** | A directory where Spark Structured Streaming saves its progress. Allows the stream to resume from where it left off. This pipeline clears checkpoints on each run to avoid stale offset conflicts during development. |
| **Row-based Window** | A Spark window that looks at a fixed number of rows before and after the current row, ordered by a column. Used here for 7-row rolling average and volatility calculations. Different from a time-based window. |
| **Watermark** | A threshold that tells Spark how long to wait for late-arriving data before closing a time window. Not explicitly used in this pipeline since `trigger(availableNow=True)` processes data in discrete runs rather than continuously. |
| **Unity Catalog** | Databricks centralised governance layer for data and AI assets. All pipeline tables are registered here under `workspace.default`. |
| **Kafka Topic** | A named stream of messages in Kafka. This pipeline uses the topic `crypto_market_ticks` on Confluent Cloud. |
| **Offset** | A unique sequential identifier for each message within a Kafka partition. Spark tracks the last processed offset in the checkpoint directory. |
| **Confluent Cloud** | A managed Kafka service running on AWS. Used in the streaming pipeline as the message broker between the producer and the Spark consumer. |
| **Databricks Secrets Scope** | A secure store for credentials inside Databricks. This pipeline uses `crypto-pipeline-secrets` to hold AWS and Confluent Cloud credentials, so they never appear in code. |
| **GitHub Actions** | A CI/CD tool that triggers automatically on code pushes. Used here to sync pipeline code to Databricks and trigger the Workflow job on every push to `main`. |

---

*L