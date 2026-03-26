-- sql/02_intermediate/int_payer_underpayment.sql
-- Description: Underpayment aggregation and remittance performance by payer and month

DROP TABLE IF EXISTS intermediate.int_payer_underpayment;

CREATE TABLE intermediate.int_payer_underpayment AS
SELECT
    DATE_TRUNC('month', claim_submitted_date)::DATE AS metric_month,
    payer_id,
    payer_name,
    payer_type,
    service_line,
    specialty,
    COUNT(DISTINCT claim_id) AS claim_count,
    COUNT(*) AS claim_line_count,
    SUM(expected_allowed_amount) AS expected_allowed_amount,
    SUM(allowed_amount) AS actual_allowed_amount,
    SUM(paid_amount) AS paid_amount,
    SUM(underpayment_amount) AS total_underpayment_amount,
    AVG(underpayment_rate) AS avg_underpayment_rate,
    SUM(CASE WHEN underpayment_amount > 0 THEN 1 ELSE 0 END) AS underpaid_line_count,
    CURRENT_TIMESTAMP AS _transformed_at
FROM intermediate.int_expected_reimbursement
GROUP BY 1, 2, 3, 4, 5, 6;

CREATE INDEX idx_int_payer_underpayment_month ON intermediate.int_payer_underpayment(metric_month);
CREATE INDEX idx_int_payer_underpayment_payer ON intermediate.int_payer_underpayment(payer_id);
