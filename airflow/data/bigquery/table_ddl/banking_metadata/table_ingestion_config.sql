CREATE OR REPLACE TABLE `banking_metadata.table_ingestion_config` (
    source_system        STRING      NOT NULL,
    source_db             STRING,
    source_table          STRING      NOT NULL,
    target_dataset        STRING      NOT NULL,
    target_table          STRING      NOT NULL,
    load_type             STRING      NOT NULL,
    primary_key           STRING      NOT NULL,
    watermark_column      STRING,
    source_path           STRING      NOT NULL,
    file_format           STRING      NOT NULL,
    is_active              BOOL        NOT NULL,
    expected_frequency     STRING,
    created_at             TIMESTAMP,
    updated_at             TIMESTAMP
);

INSERT INTO `banking_metadata.table_ingestion_config`
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
    expected_frequency,
    created_at,
    updated_at
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
    'DAILY',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
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
    'DAILY',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
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
    'HOURLY',
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
);