INSERT INTO `banking_gold.daily_transaction_summary`
SELECT
  DATE(transaction_ts) AS transaction_date,
  channel,
  COUNT(*) AS total_transactions,
  SUM(amount) AS total_amount
FROM `banking_silver.fact_transactions`
GROUP BY
  transaction_date,
  channel;