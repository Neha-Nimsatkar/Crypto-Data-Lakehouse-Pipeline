# Databricks notebook source
# quick sanity checks on the silver table before this feeds downstream


print("checking for nulls and price outliers...")

check_1_df = spark.sql("""
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN price < 0 OR price IS NULL THEN 1 ELSE 0 END) as corrupt_price_count,
        SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) as corrupt_symbol_count
    FROM workspace.default.silver_crypto_prices
""")

display(check_1_df)




print("checking ingestion delay per symbol...")

check_2_df = spark.sql("""
    SELECT 
        symbol, 
        MAX(ingestion_delay_seconds) as max_delay_secs,
        AVG(ingestion_delay_seconds) as avg_delay_secs
    FROM workspace.default.silver_crypto_prices
    GROUP BY symbol
""")

display(check_2_df)



print("checks done")