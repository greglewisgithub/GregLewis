-- sql/01_staging/stg_payments.sql
-- Description: Processes payment and adjustment transactions
-- Depends on: raw_data.payments_raw, raw_data.adjustments_raw
-- JIRA: RCM-103

DROP TABLE IF EXISTS staging.stg_payments;

CREATE TABLE staging.stg_payments AS
WITH payments AS (
    SELECT
        payment_id AS transaction_id,
        claim_id,
        payment_date AS transaction_date,
        payment_amount AS transaction_amount,
        payment_method,
        payer_id,
        'PAYMENT' AS transaction_type,
        load_date AS _loaded_at
    FROM raw_data.payments_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '90 days'
),

adjustments AS (
    SELECT
        adjustment_id AS transaction_id,
        claim_id,
        adjustment_date AS transaction_date,
        adjustment_amount AS transaction_amount,
        adjustment_reason AS payment_method,
        payer_id,
        'ADJUSTMENT' AS transaction_type,
        load_date AS _loaded_at
    FROM raw_data.adjustments_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '90 days'
),

combined AS (
    SELECT * FROM payments
    UNION ALL
    SELECT * FROM adjustments
)

SELECT
    transaction_id,
    claim_id,
    transaction_date,
    transaction_amount,
    payment_method,
    payer_id,
    transaction_type,
    _loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM combined
WHERE claim_id IS NOT NULL;

-- Indexes
CREATE INDEX idx_stg_payments_claim_id ON staging.stg_payments(claim_id);
CREATE INDEX idx_stg_payments_date ON staging.stg_payments(transaction_date);

-- Permissions
GRANT SELECT ON staging.stg_payments TO tableau_user;

-- Monitoring
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date)
SELECT 'stg_payments', COUNT(*), CURRENT_DATE FROM staging.stg_payments;
