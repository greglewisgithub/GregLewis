-- sql/03_marts/analysis_contract_renegotiation_priority.sql
-- Description: Prioritization table for managed care contracting action plans

DROP TABLE IF EXISTS analytics.analysis_contract_renegotiation_priority;

CREATE TABLE analytics.analysis_contract_renegotiation_priority AS
SELECT
    metric_month,
    payer_id,
    payer_name,
    payer_type,
    service_line,
    specialty,
    claim_count,
    total_underpayment_amount,
    denied_amount,
    avg_days_to_payment,
    underpayment_exposure_rate,
    denial_rate,
    (
        (COALESCE(total_underpayment_amount, 0) * 0.45) +
        (COALESCE(denied_amount, 0) * 0.35) +
        (COALESCE(avg_days_to_payment, 0) * 100 * 0.20)
    ) AS renegotiation_priority_score,
    CASE
        WHEN underpayment_exposure_rate > 0.08 THEN 'Rate Review + Fee Schedule Validation'
        WHEN denial_rate > 0.12 THEN 'Joint Denial Root-Cause Action Plan'
        WHEN avg_days_to_payment > 40 THEN 'Escalate Payment Timeliness Terms'
        ELSE 'Monitor'
    END AS recommended_action,
    CURRENT_TIMESTAMP AS _transformed_at
FROM analytics.metrics_payer_performance_monthly
ORDER BY renegotiation_priority_score DESC;

CREATE INDEX idx_analysis_contract_priority_month ON analytics.analysis_contract_renegotiation_priority(metric_month);
CREATE INDEX idx_analysis_contract_priority_payer ON analytics.analysis_contract_renegotiation_priority(payer_id);
