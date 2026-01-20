CREATE TABLE IF NOT EXISTS `dev-gcp-100.banking_metadata.data_quality_rules` (
    source_table       STRING      NOT NULL,
    column_name        STRING      NOT NULL,
    rule_type          STRING      NOT NULL,   -- NOT_NULL, UNIQUE, RANGE
    rule_value         STRING,                -- optional
    severity           STRING      NOT NULL,   -- ERROR / WARNING
    is_active          BOOL        NOT NULL,

    created_at         TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO `dev-gcp-100.banking_metadata.data_quality_rules`
VALUES
('customers', 'customer_id', 'NOT_NULL', NULL, 'ERROR', TRUE),
('customers', 'customer_id', 'UNIQUE', NULL, 'ERROR', TRUE),
('transactions', 'amount', 'RANGE', 'amount > 0', 'ERROR', TRUE);