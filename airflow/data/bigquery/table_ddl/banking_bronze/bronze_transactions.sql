CREATE OR REPLACE TABLE `banking_bronze.bronze_transactions` (
  transaction_id   INT64,
  account_id       INT64,
  transaction_type STRING,
  amount           STRING,   -- decimal-safe
  transaction_ts   TIMESTAMP,
  channel          STRING,
  status           STRING,
  created_at       TIMESTAMP,
  updated_at       TIMESTAMP,
  bronze_load_ts   TIMESTAMP
)
PARTITION BY DATE(bronze_load_ts)
CLUSTER BY transaction_id;