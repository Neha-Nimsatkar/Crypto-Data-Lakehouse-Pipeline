"""
File        : gold_checks.py
Location    : batch_pipeline/medallion/gold/
Description : SQL queries to visually inspect and verify the three Gold layer
              Delta tables after transformation. Run inside Databricks notebooks.

Tables Queried:
    - workspace.default.gold_latest_snapshot
    - workspace.default.gold_price_performance
    - workspace.default.gold_daily_trends

Warning:
    These are Databricks notebook SQL cells.
    %sql magic command only works inside a Databricks notebook.
"""



# ── Query 1: Latest Snapshot ───────────────────────────────────────────────────
# Shows most recent price and market cap per coin, ordered by market cap

# %sql
# SELECT
#     coin_id,
#     price_usd,
#     market_cap,
#     volume_24h,
#     event_timestamp
# FROM workspace.default.gold_latest_snapshot
# ORDER BY market_cap DESC



# ── Query 2: Price Performance (Last 24 Hours) ────────────────────────────────
# Shows moving average, volatility and market cap rank per coin

# %sql
# SELECT
#     event_timestamp,
#     coin_id,
#     price_usd,
#     moving_avg_price,
#     price_volatility,
#     market_cap_rank
# FROM workspace.default.gold_price_performance
# WHERE event_timestamp >= current_timestamp() - INTERVAL 24 HOURS
# ORDER BY event_timestamp DESC



# ── Query 3: Daily Trends ─────────────────────────────────────────────────────
# Shows daily average, max and min price per coin

# %sql
# SELECT
#     date,
#     coin_id,
#     daily_avg_price,
#     daily_max_price,
#     daily_min_price
# FROM workspace.default.gold_daily_trends
# ORDER BY date DESC, daily_avg_price DESC