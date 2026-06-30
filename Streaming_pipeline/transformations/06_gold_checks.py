# Databricks notebook source
# eyeballing the gold tables - live snapshot, hourly trend, daily rollups


print("1. latest snapshot, sorted by price...")
gold_check_1_df = spark.sql("""
    SELECT 
        coin_id,
        ROUND(price_usd, 4) as target_live_price,
        ROUND(volume_24h, 2) as aggregated_volume_usd,
        event_timestamp
    FROM workspace.default.gold_latest_snapshot
    ORDER BY target_live_price DESC
""")
display(gold_check_1_df)




print("2. last hour of price performance data...")
gold_check_2_df = spark.sql("""
    SELECT
        event_timestamp,
        coin_id,
        ROUND(price_usd, 4) as price_usd,
        ROUND(moving_avg_price, 4) as rolling_avg_price,
        ROUND(price_volatility, 6) as calculated_volatility_index,
        market_cap_rank
    FROM workspace.default.gold_price_performance
    WHERE event_timestamp >= (SELECT MAX(event_timestamp) FROM workspace.default.gold_price_performance) - INTERVAL 1 HOUR
    ORDER BY event_timestamp DESC, market_cap_rank ASC
""")
display(gold_check_2_df)




print("3. daily trends rollup...")
gold_check_3_df = spark.sql("""
    SELECT
        date,
        coin_id,
        ROUND(daily_avg_price, 4) as daily_avg,
        ROUND(daily_max_price, 4) as daily_peak,
        ROUND(daily_min_price, 4) as daily_floor,
        record_count as total_sampled_ticks
    FROM workspace.default.gold_daily_trends
    ORDER BY date DESC, daily_avg DESC
""")
display(gold_check_3_df)



print("all good, gold layer checks done")