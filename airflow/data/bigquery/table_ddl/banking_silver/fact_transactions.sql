CREATE OR REPLACE TABLE `banking_silver.fact_transactions` (
  transaction_id   STRING,
  account_id       INT64,
  customer_id      INT64,
  transaction_type STRING,
  amount           NUMERIC,
  currency         STRING,
  channel          STRING,
  merchant         STRING,
  status           STRING,
  transaction_ts   TIMESTAMP,
  source_system    STRING,
  silver_load_ts   TIMESTAMP
)
PARTITION BY DATE(transaction_ts)
CLUSTER BY account_id, customer_id;
