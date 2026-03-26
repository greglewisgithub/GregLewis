-- sql/03_marts/fct_underpayment_analytics.sql
-- Description: Monthly underpayment performance fact table for payer operations

DROP TABLE IF EXISTS analytics.fct_underpayment_analytics;

CREATE TABLE analytics.fct_underpayment_analytics AS
SELECT
    metric_month,
    payer_id,
    payer_name,
    payer_type,
    service_line,
    specialty,
    claim_count,
    claim_line_count,
    expected_allowed_amount,
    actual_allowed_amount,
    paid_amount,
    total_underpayment_amount,
    avg_underpayment_rate,
    underpaid_line_count,
    total_underpayment_amount / NULLIF(expected_allowed_amount, 0) AS underpayment_exposure_rate,
    CURRENT_TIMESTAMP AS _mart_created_at
FROM intermediate.int_payer_underpayment;

CREATE INDEX idx_fct_underpayment_month ON analytics.fct_underpayment_analytics(metric_month);
CREATE INDEX idx_fct_underpayment_payer ON analytics.fct_underpayment_analytics(payer_id);
