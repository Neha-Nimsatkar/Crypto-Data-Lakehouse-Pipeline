%sql
-- gold layer validation queries
-- checks latest snapshot, 24hr performance, and daily trends



-- 1: Latest Snapshot Validation
-- Shows the absolute latest metrics for all 16 tracked crypto assets


WITH ranked_snapshots AS (
    SELECT
        coin_id,
        price_usd,
        market_cap,
        volume_24h,
        event_timestamp,
        ROW_NUMBER() OVER (PARTITION BY coin_id ORDER BY event_timestamp DESC) as rn
    FROM workspace.default.gold_latest_snapshot
)
SELECT 
    coin_id,
    ROUND(price_usd, 4) as price_usd,
    ROUND(market_cap, 2) as market_cap_usd,
    ROUND(volume_24h, 2) as volume_24h_usd,
    event_timestamp
FROM ranked_snapshots
WHERE rn = 1
ORDER BY market_cap_usd DESC;



-- 2: Price Performance (Rolling Last 24 Hours)
-- uses inline subquery instead of session variable for Databricks compatibility


SELECT
    event_timestamp,
    coin_id,
    ROUND(price_usd, 4) as price_usd,
    ROUND(moving_avg_price, 4) as moving_avg_price_24h,
    ROUND(price_volatility, 6) as price_volatility_index,
    market_cap_rank
FROM workspace.default.gold_price_performance
WHERE event_timestamp >= (SELECT MAX(event_timestamp) FROM workspace.default.gold_price_performance) - INTERVAL 24 HOURS
ORDER BY event_timestamp DESC, market_cap_rank ASC;



-- 3: Daily Trends & Volume Quality Gates
-- Aggregated chronological trends per coin 


SELECT
    date,
    coin_id,
    ROUND(daily_avg_price, 4) as daily_avg_price,
    ROUND(daily_max_price, 4) as daily_max_price,
    ROUND(daily_min_price, 4) as daily_min_price,
    COUNT(*) as validation_ticks_sampled
FROM workspace.default.gold_daily_trends
GROUP BY date, coin_id, daily_avg_price, daily_max_price, daily_min_price
ORDER BY date DESC, daily_avg_price DESC;