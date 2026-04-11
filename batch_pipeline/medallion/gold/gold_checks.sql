%sql
SELECT 
  coin_id, 
  price_usd, 
  market_cap, 
  volume_24h, 
  event_timestamp 
FROM workspace.default.gold_latest_snapshot
ORDER BY market_cap DESC



%sql
SELECT 
  event_timestamp,
  coin_id,
  price_usd, 
  moving_avg_price,
  price_volatility,
  market_cap_rank
FROM workspace.default.gold_price_performance
WHERE event_timestamp >= current_timestamp() - INTERVAL 24 HOURS
ORDER BY event_timestamp DESC



%sql
SELECT 
  date,
  coin_id,
  daily_avg_price,
  daily_max_price,
  daily_min_price
FROM workspace.default.gold_daily_trends
ORDER BY date DESC, daily_avg_price DESC




