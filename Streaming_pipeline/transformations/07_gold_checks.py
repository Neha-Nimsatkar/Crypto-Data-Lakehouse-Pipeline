%sql
-- COMMAND ----------
-- File: notebooks/06_gold_checks.sql
-- Purpose: Visual Audit and Analytical Inspections
-- COMMAND ----------

-- Query 1: Verify the absolute latest live state metrics for all portfolio assets
SELECT 
    coin_id,
    ROUND(price_usd, 4) as target_live_price,
    ROUND(volume_24h, 2) as aggregated_volume_usd,
    event_timestamp
FROM workspace.default.gold_latest_snapshot
ORDER BY target_live_price DESC;

-- Query 2: Rolling Trend Analytics Monitor
SELECT
    event_timestamp,
    coin_id,
    ROUND(price_usd, 4) as price_usd,
    ROUND(moving_avg_price, 4) as rolling_avg_price,
    ROUND(price_volatility, 6) as calculated_volatility_index,
    market_cap_rank
FROM workspace.default.gold_price_performance
WHERE event_timestamp >= (SELECT MAX(event_timestamp) FROM workspace.default.gold_price_performance) - INTERVAL 1 HOUR
ORDER BY event_timestamp DESC, market_cap_rank ASC;

-- Query 3: Multi-Day Accumulated Structural Trends
SELECT
    date,
    coin_id,
    ROUND(daily_avg_price, 4) as daily_avg,
    ROUND(daily_max_price, 4) as daily_peak,
    ROUND(daily_min_price, 4) as daily_floor,
    record_count as total_sampled_ticks
FROM workspace.default.gold_daily_trends
ORDER BY date DESC, daily_avg DESC;
