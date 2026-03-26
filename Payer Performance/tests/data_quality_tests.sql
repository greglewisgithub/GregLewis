-- Payer Performance data quality tests

-- 1) No duplicate monthly payer rows in scorecard
SELECT metric_month, payer_id, service_line, specialty, COUNT(*) AS row_count
FROM analytics.metrics_payer_performance_monthly
GROUP BY metric_month, payer_id, service_line, specialty
HAVING COUNT(*) > 1;

-- 2) No negative expected reimbursement values
SELECT metric_month, payer_id, expected_allowed_amount
FROM analytics.fct_underpayment_analytics
WHERE expected_allowed_amount < 0;

-- 3) Priority score should not be null
SELECT metric_month, payer_id
FROM analytics.analysis_contract_renegotiation_priority
WHERE renegotiation_priority_score IS NULL;
