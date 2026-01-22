CREATE OR REPLACE TABLE `banking_metadata.data_quality_rules` (
    source_table       STRING      NOT NULL,
    column_name        STRING      NOT NULL,
    rule_type          STRING      NOT NULL,
    rule_value         STRING,
    severity           STRING      NOT NULL,
    is_active          BOOL        NOT NULL,
    created_at         TIMESTAMP
);

INSERT INTO `banking_metadata.data_quality_rules`
VALUES
('customers', 'customer_id', 'NOT_NULL', NULL, 'ERROR', TRUE, CURRENT_TIMESTAMP()),
('customers', 'customer_id', 'UNIQUE', NULL, 'ERROR', TRUE, CURRENT_TIMESTAMP()),
('transactions', 'amount', 'RANGE', 'amount > 0', 'ERROR', TRUE, CURRENT_TIMESTAMP());