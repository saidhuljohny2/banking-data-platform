INSERT INTO `banking_gold.customer_360`
SELECT
  c.customer_sk,
  c.customer_id,
  CONCAT(c.first_name, ' ', c.last_name) AS full_name,
  c.kyc_status,
  COUNT(DISTINCT a.account_sk) AS total_accounts,
  SUM(CAST(b.balance AS NUMERIC)) AS total_balance,
  MAX(t.transaction_ts) AS last_transaction_ts,
  CURRENT_DATE() AS snapshot_date
FROM `banking_silver.dim_customer` c
LEFT JOIN `banking_silver.dim_account` a
  ON c.customer_id = a.customer_id
 AND a.is_current = TRUE
LEFT JOIN `banking_bronze.bronze_accounts` b
  ON a.account_id = b.account_id
LEFT JOIN `banking_silver.fact_transactions` t
  ON c.customer_id = t.customer_id
WHERE c.is_current = TRUE
GROUP BY
  c.customer_sk,
  c.customer_id,
  full_name,
  c.kyc_status;