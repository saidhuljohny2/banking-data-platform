CREATE TABLE IF NOT EXISTS `banking_metadata.table_details` (
    dataset STRING NOT NULL,
    table STRING NOT NULL,
    create_table BOOL NOT NULL,
    create_timestamp TIMESTAMP
)
PARTITION BY DATE(create_timestamp)
OPTIONS (
    description = "Metadata table to control BigQuery table creation using DDL from GCS"
);

INSERT INTO `banking_metadata.table_details`
(dataset, table, create_table, create_timestamp)
VALUES
-- =========================
-- BRONZE LAYER
-- =========================
('banking_bronze', 'bronze_accounts', TRUE, NULL),
('banking_bronze', 'bronze_customers', TRUE, NULL),
('banking_bronze', 'bronze_transactions', TRUE, NULL),

-- =========================
-- SILVER LAYER
-- =========================
('banking_silver', 'dim_account', TRUE, NULL),
('banking_silver', 'dim_customer', TRUE, NULL),
('banking_silver', 'fact_transactions', TRUE, NULL),

-- =========================
-- GOLD LAYER
-- =========================
('banking_gold', 'customer_360', TRUE, NULL),
('banking_gold', 'daily_account_balance', TRUE, NULL),
('banking_gold', 'daily_transaction_summary', TRUE, NULL),
('banking_gold', 'fraud_indicators', TRUE, NULL),

-- =========================
-- METADATA LAYER
-- =========================
('banking_metadata', 'cdc_config', TRUE, NULL),
('banking_metadata', 'data_quality_rules', TRUE, NULL),
('banking_metadata', 'dataset_create', TRUE, NULL),
('banking_metadata', 'ingestion_audit_log', TRUE, NULL),
('banking_metadata', 'table_ingestion_config', TRUE, NULL);

-- Reset create_table flags to FALSE after initial setup
UPDATE `dev-gcp-100.banking_metadata.table_details`
SET create_table = FALSE
where create_table = True;
