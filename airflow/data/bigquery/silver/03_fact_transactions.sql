INSERT INTO `banking_silver.fact_transactions`
SELECT
  t.transaction_id,
  a.account_sk,
  c.customer_sk,
  t.transaction_type,
  CAST(t.amount AS NUMERIC),
  t.transaction_ts,
  t.channel,
  t.status,
  CURRENT_TIMESTAMP()
FROM `banking_bronze.bronze_transactions` t
JOIN `banking_silver.dim_account` a
  ON t.account_id = a.account_id
 AND a.is_current = TRUE
JOIN `banking_silver.dim_customer` c
  ON a.customer_id = c.customer_id
 AND c.is_current = TRUE;