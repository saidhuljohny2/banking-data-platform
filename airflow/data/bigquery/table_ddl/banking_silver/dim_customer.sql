CREATE OR REPLACE TABLE `banking_silver.dim_customer` (
  customer_sk STRING,
  customer_id INT64,
  first_name STRING,
  last_name STRING,
  date_of_birth DATE,
  email STRING,
  phone STRING,
  kyc_status STRING,
  effective_start_ts TIMESTAMP,
  effective_end_ts TIMESTAMP,
  is_current BOOL
)
PARTITION BY DATE(effective_start_ts)
CLUSTER BY customer_id;