-- sql/03_marts/fct_contract_compliance.sql
-- Description: Monthly payer contract compliance performance fact table

DROP TABLE IF EXISTS analytics.fct_contract_compliance;

CREATE TABLE analytics.fct_contract_compliance AS
SELECT
    metric_month,
    payer_id,
    payer_name,
    payer_type,
    claim_count,
    avg_days_to_payment,
    denied_claim_count,
    denied_amount,
    first_pass_paid_count,
    first_pass_paid_rate,
    denial_rate,
    CASE WHEN first_pass_paid_rate >= 0.90 THEN 1 ELSE 0 END AS hit_first_pass_target_flag,
    CASE WHEN avg_days_to_payment <= 30 THEN 1 ELSE 0 END AS hit_payment_timeliness_flag,
    CURRENT_TIMESTAMP AS _mart_created_at
FROM intermediate.int_contract_compliance;

CREATE INDEX idx_fct_contract_compliance_month ON analytics.fct_contract_compliance(metric_month);
CREATE INDEX idx_fct_contract_compliance_payer ON analytics.fct_contract_compliance(payer_id);
