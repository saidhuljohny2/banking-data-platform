CREATE OR REPLACE TABLE `banking_metadata.ingestion_audit_log` (
    run_id              STRING      NOT NULL,
    source_table        STRING      NOT NULL,
    target_table        STRING      NOT NULL,
    status              STRING      NOT NULL,
    records_read        INT64,
    records_written     INT64,
    start_ts            TIMESTAMP,
    end_ts              TIMESTAMP,
    error_message       STRING,
    created_at          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
);