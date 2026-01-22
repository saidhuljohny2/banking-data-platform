CREATE OR REPLACE TABLE `banking_bronze.bronze_customers` (
  customer_id      INT64,
  first_name       STRING,
  last_name        STRING,
  date_of_birth    DATE,
  email            STRING,
  phone            STRING,
  kyc_status       STRING,
  created_at       TIMESTAMP,
  updated_at       TIMESTAMP,
  bronze_load_ts   TIMESTAMP
)
PARTITION BY DATE(bronze_load_ts)
CLUSTER BY customer_id;