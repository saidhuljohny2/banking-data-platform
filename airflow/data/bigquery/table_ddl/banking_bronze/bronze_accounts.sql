CREATE OR REPLACE TABLE `banking_bronze.bronze_accounts` (
  account_id       INT64,
  customer_id      INT64,
  account_type     STRING,
  balance          STRING,   -- decimal stored safely
  currency         STRING,
  status           STRING,
  opened_date      DATE,
  created_at       TIMESTAMP,
  updated_at       TIMESTAMP,
  bronze_load_ts   TIMESTAMP
)
PARTITION BY DATE(bronze_load_ts)
CLUSTER BY account_id;