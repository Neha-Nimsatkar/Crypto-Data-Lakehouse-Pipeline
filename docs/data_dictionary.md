# Data Dictionary

Definitions for every field, metric, flag, and derived column used across the Crypto Data Lakehouse Pipeline. Use this as a reference when reading code, querying tables, or explaining the pipeline.

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

Fields that come directly from the CoinGecko API with no transformation applied.

| Field | Type | Unit | Description |
|---|---|---|---|
| `usd` | double | USD | Spot price of the coin in US dollars at the time of the API call |
| `usd_market_cap` | double | USD | Total market capitalisation — calculated as current price × circulating supply |
| `usd_24h_vol` | double | USD | Total trading volume across all exchanges in the last 24 hours |
| `last_updated_at` | long | Unix seconds | Epoch timestamp (seconds since 1970-01-01) of the last price update on CoinGecko |

---

## Derived Fields

Fields computed during Silver transformation from source fields.

| Field | Type | Derived from | Description |
|---|---|---|---|
| `coin_id` | string | JSON key name | Identifier of the coin. One of `bitcoin`, `ethereum`, or `solana`. Created when the nested JSON is unpivoted into rows |
| `price_usd` | double | `{coin}.usd` | Renamed and cast version of the raw `usd` field. Represents the coin's spot price in USD |
| `market_cap` | double | `{coin}.usd_market_cap` | Renamed version of the raw market cap field. NULL-safe — defaults to `0.0` when missing |
| `volume_24h` | double | `{coin}.usd_24h_vol` | Renamed version of the raw 24-hour volume field. NULL-safe — defaults to `0.0` when missing |
| `event_timestamp` | timestamp | `{coin}.last_updated_at` | Converted from Unix seconds to a proper timestamp. Represents when the price event occurred on CoinGecko |
| `ingested_at` | timestamp | `ingestion_metadata.ingested_at` | Parsed from ISO 8601 string to timestamp. Represents when the producer or fetcher script called the API |
| `ingestion_delay_seconds` | double | `ingested_at - event_timestamp` | Difference in seconds between when the price was recorded and when it was ingested. Negative values indicate clock sync issues and are flagged as errors |
| `date` | date | `event_timestamp` | Date portion of `event_timestamp`. Used as the partition column in batch Silver table |
| `fallback_time` | long | `unix_timestamp()` | Spark system time used as a fallback when `last_updated_at` is NULL in the streaming Bronze table. Dropped after transformation |

---

## Flag Fields

Boolean or categorical fields that indicate data quality or business conditions.

| Field | Type | Possible values | Description |
|---|---|---|---|
| `price_change_flag` | string | `UP`, `DOWN`, `STABLE` | Indicates the direction of price movement compared to the previous record for the same coin. Computed using a window function ordered by `event_timestamp` partitioned by `coin_id` |

**Logic for `price_change_flag`:**

```
current_price > previous_price  →  UP
current_price < previous_price  →  DOWN
current_price = previous_price  →  STABLE
previous_price IS NULL          →  STABLE  (first record for that coin)
```

---

## Aggregated Metrics

Fields produced by aggregation in Gold layer transformations.

| Field | Type | Computed in | Description |
|---|---|---|---|
| `moving_avg_price` | double | `gold_price_performance` | Rolling average of `price_usd`. Batch: 7-period row-based window. Streaming: 5-minute sliding window with 1-minute slide. Rounded to 4 decimal places |
| `price_volatility` | double | `gold_price_performance` | Rolling standard deviation of `price_usd` within the same window as `moving_avg_price`. Higher values indicate more unstable prices. Rounded to 4 decimal places. NULL when only 1 record exists in the window |
| `market_cap_rank` | int | `gold_price_performance` | Rank of each coin by `market_cap` per timestamp. `1` = largest market cap. Computed using `RANK()` window function partitioned by `event_timestamp` ordered by `market_cap` descending |
| `daily_avg_price` | double | `gold_daily_trends` | Average `price_usd` for all records within the calendar day or 24-hour window. Rounded to 4 decimal places |
| `daily_max_price` | double | `gold_daily_trends` | Maximum `price_usd` recorded within the calendar day or 24-hour window. Rounded to 4 decimal places |
| `daily_min_price` | double | `gold_daily_trends` | Minimum `price_usd` recorded within the calendar day or 24-hour window. Rounded to 4 decimal places |
| `daily_avg_volume` | double | `gold_daily_trends` | Average `volume_24h` across all records within the calendar day or 24-hour window. Rounded to 2 decimal places |
| `record_count` | long | `gold_daily_trends` (batch only) | Number of Silver records that contributed to the daily aggregate for that coin and day |

---

## Window Fields

Fields that define time windows used in streaming Gold aggregations.

| Field | Type | Table | Description |
|---|---|---|---|
| `window_start` | timestamp | `gold/daily_trends` (streaming) | Start timestamp of the 24-hour tumbling window |
| `start_time` | timestamp | `gold/price_performance` (streaming) | Start timestamp of the 5-minute sliding window |

**Window types used:**

| Window type | Parameters | Used in |
|---|---|---|
| Tumbling window | 24 hours | Streaming `gold/daily_trends` |
| Sliding window | 5 minutes duration, 1 minute slide | Streaming `gold/price_performance` |
| Row-based window | 7 rows preceding to current | Batch `gold_price_performance` |
| Partition window | Latest row per coin | `gold_latest_snapshot` (both pipelines) |

---

## Metadata Fields

Fields that track lineage, provenance, and system information.

| Field | Type | Present in | Description |
|---|---|---|---|
| `ingestion_metadata.source` | string | Bronze (both) | Origin of the data. Always `CoinGecko API` |
| `ingestion_metadata.ingested_at` | string | Bronze (both) | ISO 8601 UTC timestamp string set by the producer or fetcher script at the moment the API was called |
| `ingestion_metadata.coins` | string | Bronze (batch) | Comma-separated list of coins requested from the API. Example: `bitcoin,ethereum,solana` |
| `ingested_at` | timestamp | Silver (streaming) | Parsed timestamp version of `ingestion_metadata.ingested_at` |
| `ingestion_delay_seconds` | double | Silver (batch) | See Derived Fields above |

---

## Technical Terms

Definitions for technical concepts referenced in code, READMEs, and documentation.

| Term | Definition |
|---|---|
| **Medallion architecture** | A data design pattern that organises data into three progressive layers — Bronze (raw), Silver (cleaned), Gold (aggregated) — each increasing in quality and business value |
| **Delta Lake** | An open-source storage layer that adds ACID transactions, schema enforcement, time travel, and upsert support on top of Parquet files stored in S3 or local storage |
| **Structured Streaming** | Apache Spark's engine for processing continuous data streams using the same DataFrame API as batch processing. Used for Bronze, Silver, and Gold streaming jobs |
| **Watermark** | A threshold that tells Spark Structured Streaming how long to wait for late-arriving data before closing a time window. Set to 10 minutes in this pipeline |
| **Tumbling window** | A non-overlapping fixed-size time window. Each event belongs to exactly one window. Used for daily trend aggregations |
| **Sliding window** | An overlapping time window that advances by a slide interval smaller than its duration. Each event can belong to multiple windows. Used for moving average calculation |
| **foreachBatch** | A Spark Structured Streaming output mode that processes each micro-batch as a static DataFrame. Used for the latest snapshot upsert logic |
| **Checkpoint** | A directory where Spark Structured Streaming saves its progress (processed offsets, state). Allows the stream to resume from where it left off after a restart |
| **Upsert** | A write operation that updates existing rows if they match a key, or inserts new rows if they do not. Used in latest snapshot to always keep only the most recent price per coin |
| **Stack expression** | A Spark SQL `stack(n, ...)` expression that converts multiple columns into rows. Used in Silver transformation to unpivot the three nested coin objects into one row per coin |
| **Partition** | A way of physically organising data on storage by the values of a column. The batch Silver table is partitioned by `date` so queries filtering by date scan less data |
| **Unity Catalog** | Databricks centralised governance layer for data and AI assets. Batch Silver and Gold tables are registered in Unity Catalog under `workspace.default` |
| **Kafka topic** | A named stream of messages in Kafka. This pipeline uses the topic `crypto_prices` where the producer publishes and the consumer subscribes |
| **Offset** | A unique sequential identifier for each message within a Kafka partition. Spark tracks the last processed offset in the checkpoint directory to avoid reprocessing |
| **Bootstrap server** | The initial Kafka broker address a client connects to in order to discover the full cluster. Set to `localhost:9092` in this pipeline |

---

*Last updated: April 2026 — Crypto Data Lakehouse Pipeline*

