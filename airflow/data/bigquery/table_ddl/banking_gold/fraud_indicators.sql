CREATE OR REPLACE TABLE `banking_gold.fraud_indicators` (
  account_sk INT64,
  suspicious_txn_count INT64,
  suspicious_amount NUMERIC,
  flag_reason STRING,
  snapshot_date DATE
)
PARTITION BY snapshot_date;