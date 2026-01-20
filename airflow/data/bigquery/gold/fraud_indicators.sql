INSERT INTO `banking_gold.fraud_indicators`
SELECT
  account_sk,
  COUNT(*) AS suspicious_txn_count,
  SUM(amount) AS suspicious_amount,
  'HIGH_VALUE_TXNS' AS flag_reason,
  CURRENT_DATE() AS snapshot_date
FROM `banking_silver.fact_transactions`
WHERE amount > 100000
GROUP BY account_sk;