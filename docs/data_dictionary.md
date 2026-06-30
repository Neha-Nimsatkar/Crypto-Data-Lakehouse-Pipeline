# Data Dictionary

Definitions for every field, metric, flag, and derived column used across the Crypto Data Lakehouse Pipeline. Use this as a reference when reading code, querying tables, or explaining the pipeline architecture.

---

## Table of Contents

- [Source Fields](#source-fields)
- [Derived Fields](#derived-fields)
- [Flag Fields](#flag-fields)
- [Aggregated Metrics](#aggregated-metrics)
- [Window Fields](#window-fields)
- [Metadata Fields](#metadata-fields)
- [Technical Terms](#technical-terms)

---

## Source Fields

Fields that come directly from the CoinGecko API or raw Kafka messages with no schema transformation applied.

| Field | Type | Unit | Description |
|---|---|---|---|
| `usd` | double | USD | Spot price of the coin in US dollars at the time of the API call or simulated tick. |
| `usd_market_cap` | double | USD | Total market capitalization — calculated as current price $	imes$ circulating supply. |
| `usd_24h_vol` | double | USD | Total trading volume across all exchanges in the last 24 hours. |
| `last_updated_at` | long | Unix seconds | Epoch timestamp (seconds since 1970-01-01) of the last price update on the exchange layer. |

---

## Derived Fields

Fields computed during Silver transformation stages from raw incoming source payloads.

| Field | Type | Derived from | Description |
|---|---|---|---|
| `coin_id` | string | JSON key name | Identifier of the coin (e.g., `bitcoin`, `ethereum`, `solana`). Extracted when nested payloads are unpivoted into row records. |
| `price_usd` | double | `{coin}.usd` | Renamed and cast version of the raw `usd` field. Represents the coin's spot price in USD. |
| `market_cap` | double | `{coin}.usd_market_cap` | Renamed version of the raw market cap field. NULL-safe — defaults to `0.0` when missing. |
| `volume_24h` | double | `{coin}.usd_24h_vol` | Renamed version of the raw 24-hour volume field. NULL-safe — defaults to `0.0` when missing. |
| `event_timestamp` | timestamp | `{coin}.last_updated_at` | Converted from Unix seconds to a proper timestamp. Represents when the price event occurred at the source. |
| `ingested_at` | timestamp | `ingestion_metadata.ingested_at` | Parsed from ISO 8601 string to timestamp. Represents when the producer or fetcher script called the API. |
| `ingestion_delay_seconds` | double | `ingested_at - event_timestamp` | Difference in seconds between when the price was recorded and when it was ingested. Negative values indicate clock sync issues and are flagged as errors. |
| `date` | date | `event_timestamp` | Date portion of `event_timestamp`. Used as a physical partition column for S3 file storage efficiency. |
| `fallback_time` | long | `unix_timestamp()` | Spark system time used as a fallback when `last_updated_at` is NULL in the streaming Bronze table. Dropped after transformation. |

---

## Flag Fields

Boolean or categorical fields that indicate data quality or business conditions.

| Field | Type | Possible values | Description |
|---|---|---|---|
| `price_change_flag` | string | `UP`, `DOWN`, `STABLE` | Indicates the direction of price movement compared to the previous record for the same coin. Computed using a window function ordered by `event_timestamp` partitioned by `coin_id`. |

**Logic for `price_change_flag`:**

```
current_price > previous_price  →  UP
current_price < previous_price  →  DOWN
current_price = previous_price  →  STABLE
previous_price IS NULL          →  STABLE  (first record for that coin)
```

---

## Aggregated Metrics

Fields produced by mathematical aggregation steps within the Gold layer transformations.

| Field | Type | Computed in | Description |
|---|---|---|---|
| `moving_avg_price` | double | `gold_price_performance` | Rolling average of `price_usd`. Batch: 7-period row-based window. Streaming: 5-minute sliding window with 1-minute slide. Rounded to 4 decimal places. |
| `price_volatility` | double | `gold_price_performance` | Rolling standard deviation of `price_usd` within the same window as `moving_avg_price`. Higher values indicate more unstable prices. Rounded to 4 decimal places. NULL when only 1 record exists in the window. |
| `market_cap_rank` | int | `gold_price_performance` | Rank of each coin by `market_cap` per timestamp. `1` = largest market cap. Computed using `RANK()` window function partitioned by `event_timestamp` ordered by `market_cap` descending. |
| `daily_avg_price` | double | `gold_daily_trends` | Average `price_usd` for all records within the calendar day or 24-hour window. Rounded to 4 decimal places. |
| `daily_max_price` | double | `gold_daily_trends` | Maximum `price_usd` recorded within the calendar day or 24-hour window. Rounded to 4 decimal places. |
| `daily_min_price` | double | `gold_daily_trends` | Minimum `price_usd` recorded within the calendar day or 24-hour window. Rounded to 4 decimal places. |
| `daily_avg_volume` | double | `gold_daily_trends` | Average `volume_24h` across all records within the calendar day or 24-hour window. Rounded to 2 decimal places. |
| `record_count` | long | `gold_daily_trends` (batch only) | Number of Silver records that contributed to the daily aggregate for that coin and day. |

---

## Window Fields

Fields that define time structures used across processing aggregations.

| Field | Type | Table Context | Description |
|---|---|---|---|
| `window_start` | timestamp | `gold_daily_trends` (streaming) | Start timestamp of the 24-hour tumbling window. |
| `start_time` | timestamp | `gold_price_performance` (streaming) | Start timestamp of the 5-minute sliding window. |

**Window configurations applied:**

| Window Type | Parameters | Used In Target Table |
|---|---|---|
| **Tumbling Window** | 24 hours fixed | Streaming `gold_stream_daily_trends` |
| **Sliding Window** | 5 minutes duration, 1 minute slide | Streaming `gold_stream_price_performance` |
| **Row-Based Window** | 7 rows preceding to current row | Batch `gold_price_performance` |
| **Partition Window** | Latest row per unique `coin_id` | Both `latest_snapshot` target engines |

---

## Metadata Fields

Fields that track lineage, data provenance, and underlying system information.

| Field | Type | Present in | Description |
|---|---|---|---|
| `ingestion_metadata.source` | string | Bronze (both) | Origin of the data stream asset block. |
| `ingestion_metadata.ingested_at` | string | Bronze (both) | ISO 8601 UTC timestamp string set by the script execution at the moment the ingest loop kicked off. |
| `ingestion_metadata.coins` | string | Bronze (batch) | Comma-separated string list of asset tokens requested (e.g., `bitcoin,ethereum,solana`). |
| `ingested_at` | timestamp | Silver (streaming) | Parsed, indexable timestamp version of `ingestion_metadata.ingested_at`. |
| `ingestion_delay_seconds` | double | Silver (batch) | Evaluated system execution timestamp processing offset difference metric. |

---

## Technical Terms

Definitions for specialized concepts referenced across processing code configurations and pipeline tasks.

| Term | Definition |
|---|---|
| **Medallion Architecture** | A data design pattern that organizes data into three progressive layers — Bronze (raw), Silver (cleaned), Gold (aggregated) — each increasing in data quality and business value. |
| **Delta Lake** | An open-source storage layer that adds ACID transactions, schema enforcement, time travel, and upsert support on top of Parquet files stored in S3 or local storage. |
| **Structured Streaming** | Apache Spark's engine for processing continuous data streams using the same DataFrame API as batch processing. Used for Bronze, Silver, and Gold streaming jobs. |
| **Watermark** | A threshold that tells Spark Structured Streaming how long to wait for late-arriving data before closing a time window. Set to 10 minutes in this pipeline to handle late data arrivals. |
| **Tumbling Window** | A non-overlapping fixed-size time window. Each event belongs to exactly one window. Used for daily trend aggregations. |
| **Sliding Window** | An overlapping time window that advances by a slide interval smaller than its duration. Each event can belong to multiple windows. Used for moving average calculations. |
| **foreachBatch** | A Spark Structured Streaming output mode that processes each micro-batch as a static DataFrame. Used for the latest snapshot upsert logic. |
| **Checkpoint** | A directory where Spark Structured Streaming saves its progress (processed offsets, state information). Allows the stream to resume from where it left off after a cluster restart. |
| **Upsert** | A write operation that updates existing rows if they match a key, or inserts new rows if they do not. Used in latest snapshot to always keep only the most recent price per coin. |
| **Stack Expression** | A Spark SQL `stack(n, ...)` expression that converts multiple columns into rows. Used in Silver transformation to unpivot the nested coin objects into one row per coin. |
| **Partition** | A way of physically organizing data on storage by the values of a column. The batch Silver table is partitioned by `date` so queries filtering by date scan less data. |
| **Unity Catalog** | Databricks centralized governance layer for data and AI assets. Tables are registered securely under Unity Catalog Volumes. |
| **Kafka Topic** | A named stream of messages in Kafka. This pipeline uses designated crypto tracking topics on Confluent Cloud. |
| **Offset** | A unique sequential identifier for each message within a Kafka partition. Spark tracks the last processed offset in the checkpoint directory to avoid data reprocessing. |

---

*Last updated: April 2026 — Crypto Data Lakehouse Pipeline*
