MERGE `banking_silver.dim_account` tgt
USING (
  SELECT
    account_id,
    customer_id,
    account_type,
    currency,
    status,
    opened_date,
    CURRENT_TIMESTAMP() AS start_ts
  FROM `banking_bronze.bronze_accounts`
) src
ON tgt.account_id = src.account_id
AND tgt.is_current = TRUE

WHEN MATCHED AND (
  tgt.account_type != src.account_type OR
  tgt.status != src.status OR
  tgt.customer_id != src.customer_id
)
THEN
  UPDATE SET
    tgt.effective_end_ts = src.start_ts,
    tgt.is_current = FALSE

WHEN NOT MATCHED BY TARGET
THEN
  INSERT (
    account_sk,
    account_id,
    customer_id,
    account_type,
    currency,
    status,
    opened_date,
    effective_start_ts,
    effective_end_ts,
    is_current
  )
  VALUES (
    GENERATE_UUID(),
    src.account_id,
    src.customer_id,
    src.account_type,
    src.currency,
    src.status,
    src.opened_date,
    src.start_ts,
    TIMESTAMP('9999-12-31'),
    TRUE
  );