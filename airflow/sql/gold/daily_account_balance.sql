INSERT INTO `banking_gold.daily_account_balance`
SELECT
  a.account_sk,
  a.account_id,
  a.customer_id,
  CAST(b.balance AS NUMERIC) AS balance,
  CURRENT_DATE() AS balance_date
FROM `banking_silver.dim_account` a
JOIN `banking_bronze.bronze_accounts` b
  ON a.account_id = b.account_id
WHERE a.is_current = TRUE;