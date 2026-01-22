CREATE OR REPLACE TABLE `banking_metadata.cdc_config` (
  source STRING,
  table_name STRING,
  primary_key STRING,
  watermark_column STRING,
  is_active BOOL,
  last_success_ts TIMESTAMP
);

INSERT INTO `banking_metadata.cdc_config` VALUES
('cloudsql', 'customers', 'customer_id', 'updated_at', TRUE, '1970-01-01'),
('cloudsql', 'accounts', 'account_id', 'updated_at', TRUE, '1970-01-01'),
('cloudsql', 'transactions', 'transaction_id', 'updated_at', TRUE, '1970-01-01');