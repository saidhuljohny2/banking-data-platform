CREATE TABLE IF NOT EXISTS `dev-gcp-100.banking_metadata.table_ingestion_config` (
    source_system        STRING      NOT NULL,   -- cloudsql, api, files
    source_db             STRING,                 -- banking_db
    source_table          STRING      NOT NULL,   -- customers
    target_dataset        STRING      NOT NULL,   -- banking_bronze
    target_table          STRING      NOT NULL,   -- bronze_customers

    load_type             STRING      NOT NULL,   -- FULL / INCREMENTAL
    primary_key           STRING      NOT NULL,   -- customer_id
    watermark_column      STRING,                 -- updated_at

    source_path           STRING      NOT NULL,   -- GCS path
    file_format           STRING      NOT NULL,   -- parquet / avro / json

    is_active              BOOL        NOT NULL,
    expected_frequency     STRING,                -- DAILY / HOURLY

    created_at             TIMESTAMP   DEFAULT CURRENT_TIMESTAMP(),
    updated_at             TIMESTAMP
);

INSERT INTO `dev-gcp-100.banking_metadata.table_ingestion_config`
(
    source_system,
    source_db,
    source_table,
    target_dataset,
    target_table,
    load_type,
    primary_key,
    watermark_column,
    source_path,
    file_format,
    is_active,
    expected_frequency
)
VALUES
(
    'cloudsql',
    'banking_db',
    'customers',
    'banking_bronze',
    'bronze_customers',
    'INCREMENTAL',
    'customer_id',
    'updated_at',
    'gs://banking-raw-dev-100/cloudsql/customers/',
    'parquet',
    TRUE,
    'DAILY'
),
(
    'cloudsql',
    'banking_db',
    'accounts',
    'banking_bronze',
    'bronze_accounts',
    'INCREMENTAL',
    'account_id',
    'updated_at',
    'gs://banking-raw-dev-100/cloudsql/accounts/',
    'parquet',
    TRUE,
    'DAILY'
),
(
    'cloudsql',
    'banking_db',
    'transactions',
    'banking_bronze',
    'bronze_transactions',
    'INCREMENTAL',
    'transaction_id',
    'updated_at',
    'gs://banking-raw-dev-100/cloudsql/transactions/',
    'parquet',
    TRUE,
    'HOURLY'
);