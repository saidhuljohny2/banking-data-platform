CREATE OR REPLACE TABLE `banking_silver.fact_transactions` (
  transaction_id INT64,
  account_sk STRING,
  customer_sk STRING,
  transaction_type STRING,
  amount NUMERIC,
  transaction_ts TIMESTAMP,
  channel STRING,
  status STRING,
  load_ts TIMESTAMP
)
PARTITION BY DATE(transaction_ts)
CLUSTER BY account_sk, customer_sk;