CREATE OR REPLACE TABLE `banking_gold.daily_transaction_summary` (
  transaction_date DATE,
  channel STRING,
  total_transactions INT64,
  total_amount NUMERIC
)
PARTITION BY transaction_date
CLUSTER BY channel;