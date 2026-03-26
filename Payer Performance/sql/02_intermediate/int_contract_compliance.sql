-- sql/02_intermediate/int_contract_compliance.sql
-- Description: Contract adherence signals combining denial, timeliness, and payment behavior

DROP TABLE IF EXISTS intermediate.int_contract_compliance;

CREATE TABLE intermediate.int_contract_compliance AS
WITH claim_summary AS (
    SELECT
        claim_id,
        payer_id,
        payer_name,
        payer_type,
        claim_submitted_date,
        days_to_payment,
        is_denied,
        denied_amount,
        is_first_pass_paid
    FROM analytics.fct_claims_lifecycle
)
SELECT
    DATE_TRUNC('month', claim_submitted_date)::DATE AS metric_month,
    payer_id,
    payer_name,
    payer_type,
    COUNT(*) AS claim_count,
    AVG(days_to_payment) AS avg_days_to_payment,
    SUM(is_denied) AS denied_claim_count,
    SUM(denied_amount) AS denied_amount,
    SUM(is_first_pass_paid) AS first_pass_paid_count,
    SUM(is_first_pass_paid)::NUMERIC / NULLIF(COUNT(*), 0) AS first_pass_paid_rate,
    SUM(is_denied)::NUMERIC / NULLIF(COUNT(*), 0) AS denial_rate,
    CURRENT_TIMESTAMP AS _transformed_at
FROM claim_summary
GROUP BY 1, 2, 3, 4;

CREATE INDEX idx_int_contract_compliance_month ON intermediate.int_contract_compliance(metric_month);
CREATE INDEX idx_int_contract_compliance_payer ON intermediate.int_contract_compliance(payer_id);
