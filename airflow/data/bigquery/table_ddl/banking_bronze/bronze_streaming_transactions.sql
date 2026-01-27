CREATE TABLE IF NOT EXISTS `banking_bronze.bronze_streaming_transactions` (
  event_id STRING,
  transaction_id STRING,
  account_id INT64,
  customer_id INT64,
  amount NUMERIC,
  currency STRING,
  transaction_type STRING,
  channel STRING,
  merchant STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP
)
PARTITION BY DATE(event_ts)
CLUSTER BY account_id, channel;
