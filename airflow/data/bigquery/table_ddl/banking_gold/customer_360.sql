CREATE OR REPLACE TABLE `banking_gold.customer_360` (
  customer_sk STRING,
  customer_id INT64,
  full_name STRING,
  kyc_status STRING,
  total_accounts INT64,
  total_balance NUMERIC,
  last_transaction_ts TIMESTAMP,
  snapshot_date DATE
)
PARTITION BY snapshot_date
CLUSTER BY customer_id;