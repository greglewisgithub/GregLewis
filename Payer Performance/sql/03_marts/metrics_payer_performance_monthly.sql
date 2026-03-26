-- sql/03_marts/metrics_payer_performance_monthly.sql
-- Description: Executive monthly payer performance scorecard

DROP TABLE IF EXISTS analytics.metrics_payer_performance_monthly;

CREATE TABLE analytics.metrics_payer_performance_monthly AS
SELECT
    u.metric_month,
    u.payer_id,
    u.payer_name,
    u.payer_type,
    u.service_line,
    u.specialty,
    u.claim_count,
    u.expected_allowed_amount,
    u.actual_allowed_amount,
    u.paid_amount,
    u.total_underpayment_amount,
    u.underpayment_exposure_rate,
    c.avg_days_to_payment,
    c.denied_claim_count,
    c.denied_amount,
    c.first_pass_paid_rate,
    c.denial_rate,
    CASE WHEN u.underpayment_exposure_rate > 0.05 THEN 1 ELSE 0 END AS underpayment_alert_flag,
    CASE WHEN c.denial_rate > 0.10 THEN 1 ELSE 0 END AS denial_alert_flag,
    CASE WHEN c.avg_days_to_payment > 35 THEN 1 ELSE 0 END AS payment_delay_alert_flag,
    CURRENT_TIMESTAMP AS _transformed_at
FROM analytics.fct_underpayment_analytics u
LEFT JOIN analytics.fct_contract_compliance c
  ON u.metric_month = c.metric_month
 AND u.payer_id = c.payer_id;

CREATE INDEX idx_metrics_payer_month ON analytics.metrics_payer_performance_monthly(metric_month);
CREATE INDEX idx_metrics_payer_name ON analytics.metrics_payer_performance_monthly(payer_name);
