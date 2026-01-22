CREATE OR REPLACE TABLE `banking_silver.dim_account` (
  account_sk STRING,
  account_id INT64,
  customer_id INT64,
  account_type STRING,
  currency STRING,
  status STRING,
  opened_date DATE,
  effective_start_ts TIMESTAMP,
  effective_end_ts TIMESTAMP,
  is_current BOOL
)
PARTITION BY DATE(effective_start_ts)
CLUSTER BY account_id;