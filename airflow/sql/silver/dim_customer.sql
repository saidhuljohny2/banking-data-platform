MERGE `banking_silver.dim_customer` tgt
USING (
  SELECT
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    email,
    phone,
    kyc_status,
    CURRENT_TIMESTAMP() AS start_ts
  FROM `banking_bronze.bronze_customers`
) src
ON tgt.customer_id = src.customer_id
AND tgt.is_current = TRUE

WHEN MATCHED AND (
  tgt.first_name != src.first_name OR
  tgt.last_name != src.last_name OR
  tgt.email != src.email OR
  tgt.phone != src.phone OR
  tgt.kyc_status != src.kyc_status
)
THEN
  UPDATE SET
    tgt.effective_end_ts = src.start_ts,
    tgt.is_current = FALSE

WHEN NOT MATCHED BY TARGET
THEN
  INSERT (
    customer_sk,
    customer_id,
    first_name,
    last_name,
    date_of_birth,
    email,
    phone,
    kyc_status,
    effective_start_ts,
    effective_end_ts,
    is_current
  )
  VALUES (
    GENERATE_UUID(),
    src.customer_id,
    src.first_name,
    src.last_name,
    src.date_of_birth,
    src.email,
    src.phone,
    src.kyc_status,
    src.start_ts,
    TIMESTAMP('9999-12-31'),
    TRUE
  );