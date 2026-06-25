%sql
--04 silver validation
-- COMMAND ----------
-- File: notebooks/03_silver_recheck.sql
-- Purpose: Post-execution audit and data observability checks (Silver Gate Verification)
-- COMMAND ----------

-- Check 1: Ensure absolutely NO null keys or impossible outliers leaked into production
SELECT 
    COUNT(*) as total_rows,
    SUM(CASE WHEN price < 0 OR price IS NULL THEN 1 ELSE 0 END) as corrupt_price_count,
    SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) as corrupt_symbol_count
FROM workspace.default.silver_crypto_prices;

-- Check 2: Verify dynamic ingestion pipeline delay and clock synchronization metrics
SELECT 
    symbol, 
    MAX(ingestion_delay_seconds) as max_delay_secs,
    AVG(ingestion_delay_seconds) as avg_delay_secs
FROM workspace.default.silver_crypto_prices
GROUP BY symbol;
