CREATE OR REPLACE TABLE `banking_gold.daily_account_balance` (
  account_sk STRING,
  account_id INT64,
  customer_id INT64,
  balance NUMERIC,
  balance_date DATE
)
PARTITION BY balance_date
CLUSTER BY account_id;