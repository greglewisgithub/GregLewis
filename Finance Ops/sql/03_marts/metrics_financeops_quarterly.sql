-- sql/03_marts/metrics_financeops_quarterly.sql
-- Description: Quarterly Finance Ops KPI rollup for executive operating reviews

DROP TABLE IF EXISTS analytics.metrics_financeops_quarterly;

CREATE TABLE analytics.metrics_financeops_quarterly AS
SELECT
    DATE_TRUNC('quarter', metric_month)::DATE AS metric_quarter,
    segment,
    region,
    SUM(blended_forecast_amount) AS blended_forecast_amount,
    SUM(realized_bookings) AS realized_bookings,
    AVG(avg_forecast_accuracy_ratio) AS avg_forecast_accuracy_ratio,
    SUM(total_quota) AS total_quota,
    SUM(total_bookings) AS total_bookings,
    AVG(avg_attainment_rate) AS avg_attainment_rate,
    SUM(reps_hitting_quota) AS reps_hitting_quota,
    SUM(total_budget) AS total_budget,
    SUM(total_actuals) AS total_actuals,
    SUM(total_variance) AS total_variance,
    CURRENT_TIMESTAMP AS _transformed_at
FROM analytics.metrics_financeops_monthly
GROUP BY 1, 2, 3;

CREATE INDEX idx_metrics_financeops_quarterly_qtr ON analytics.metrics_financeops_quarterly(metric_quarter);
