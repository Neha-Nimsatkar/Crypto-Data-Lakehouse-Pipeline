# Databricks notebook source
#checking the silver layer tables and validating it 


print(" 1: Checking for NULLs and Outliers...")

check_1_df = spark.sql("""
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN price < 0 OR price IS NULL THEN 1 ELSE 0 END) as corrupt_price_count,
        SUM(CASE WHEN symbol IS NULL THEN 1 ELSE 0 END) as corrupt_symbol_count
    FROM workspace.default.silver_crypto_prices
""")

display(check_1_df)




print(" 2: Verifying Pipeline Ingestion Delay...")

check_2_df = spark.sql("""
    SELECT 
        symbol, 
        MAX(ingestion_delay_seconds) as max_delay_secs,
        AVG(ingestion_delay_seconds) as avg_delay_secs
    FROM workspace.default.silver_crypto_prices
    GROUP BY symbol
""")

display(check_2_df)



print(" All validation queries successfully executed!")
