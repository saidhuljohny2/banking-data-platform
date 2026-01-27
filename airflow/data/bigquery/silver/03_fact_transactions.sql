MERGE `banking_silver.fact_transactions` T
USING (

  WITH unified_transactions AS (

    -- =================================================
    -- 1️⃣ BATCH TRANSACTIONS (Cloud SQL → Bronze)
    -- =================================================
    SELECT
      CAST(transaction_id AS STRING) as transaction_id,
      bt.account_id,
      a.customer_id,
      transaction_type,
      CAST(amount AS NUMERIC) AS amount,
      NULL AS currency,
      channel,
      NULL AS merchant,
      bt.status,
      transaction_ts,
      "BATCH" AS source_system,
      1 AS source_priority
    FROM `banking_bronze.bronze_transactions` bt
    JOIN `banking_bronze.bronze_accounts` a
      ON bt.account_id = a.account_id

    UNION ALL

    -- =================================================
    -- 2️⃣ STREAMING TRANSACTIONS (Pub/Sub → Bronze)
    -- =================================================
    SELECT
      transaction_id,
      account_id,
      customer_id,
      transaction_type,
      amount,
      currency,
      channel,
      merchant,
      NULL AS status,
      event_ts AS transaction_ts,
      "STREAMING" AS source_system,
      2 AS source_priority
    FROM `banking_bronze.bronze_streaming_transactions`
  ),

  -- =================================================
  -- 3️⃣ DEDUPLICATION (BATCH WINS)
  -- =================================================
  deduplicated AS (
    SELECT *
    FROM (
      SELECT *,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY source_priority ASC, transaction_ts DESC
        ) AS rn
      FROM unified_transactions
    )
    WHERE rn = 1
  )

  SELECT * FROM deduplicated

) S
ON T.transaction_id = S.transaction_id

-- =====================================================
-- UPDATE EXISTING RECORD
-- =====================================================
WHEN MATCHED THEN
  UPDATE SET
    T.account_id       = S.account_id,
    T.customer_id      = S.customer_id,
    T.transaction_type = S.transaction_type,
    T.amount           = S.amount,
    T.currency         = S.currency,
    T.channel          = S.channel,
    T.merchant         = S.merchant,
    T.status           = S.status,
    T.transaction_ts   = S.transaction_ts,
    T.source_system    = S.source_system,
    T.silver_load_ts   = CURRENT_TIMESTAMP()

-- =====================================================
-- INSERT NEW RECORD
-- =====================================================
WHEN NOT MATCHED THEN
  INSERT (
    transaction_id,
    account_id,
    customer_id,
    transaction_type,
    amount,
    currency,
    channel,
    merchant,
    status,
    transaction_ts,
    source_system,
    silver_load_ts
  )
  VALUES (
    S.transaction_id,
    S.account_id,
    S.customer_id,
    S.transaction_type,
    S.amount,
    S.currency,
    S.channel,
    S.merchant,
    S.status,
    S.transaction_ts,
    S.source_system,
    CURRENT_TIMESTAMP()
  );
