-- sql/03_marts/metrics_financeops_monthly.sql
-- Description: Monthly executive KPI aggregates for Finance Ops

DROP TABLE IF EXISTS analytics.metrics_financeops_monthly;

CREATE TABLE analytics.metrics_financeops_monthly AS
WITH forecast AS (
    SELECT
        forecast_month AS metric_month,
        segment,
        region,
        SUM(blended_forecast_amount) AS blended_forecast_amount,
        SUM(realized_bookings) AS realized_bookings,
        AVG(forecast_accuracy_ratio) AS avg_forecast_accuracy_ratio
    FROM analytics.fct_revenue_forecast_lifecycle
    GROUP BY 1, 2, 3
),
attainment AS (
    SELECT
        quota_month AS metric_month,
        segment,
        region,
        SUM(quota_amount) AS total_quota,
        SUM(bookings_amount) AS total_bookings,
        AVG(attainment_rate) AS avg_attainment_rate,
        SUM(hit_quota_flag) AS reps_hitting_quota
    FROM analytics.fct_bookings_attainment
    GROUP BY 1, 2, 3
),
budget AS (
    SELECT
        fiscal_month AS metric_month,
        SUM(planned_amount) AS total_budget,
        SUM(actual_amount) AS total_actuals,
        SUM(variance_amount) AS total_variance
    FROM analytics.fct_budget_actual_variance
    GROUP BY 1
)
SELECT
    f.metric_month,
    f.segment,
    f.region,
    f.blended_forecast_amount,
    f.realized_bookings,
    f.avg_forecast_accuracy_ratio,
    COALESCE(a.total_quota, 0) AS total_quota,
    COALESCE(a.total_bookings, 0) AS total_bookings,
    COALESCE(a.avg_attainment_rate, 0) AS avg_attainment_rate,
    COALESCE(a.reps_hitting_quota, 0) AS reps_hitting_quota,
    COALESCE(b.total_budget, 0) AS total_budget,
    COALESCE(b.total_actuals, 0) AS total_actuals,
    COALESCE(b.total_variance, 0) AS total_variance,
    CASE WHEN f.avg_forecast_accuracy_ratio >= 0.95 THEN 1 ELSE 0 END AS on_target_forecast_flag,
    CASE WHEN COALESCE(a.avg_attainment_rate, 0) >= 1 THEN 1 ELSE 0 END AS on_target_attainment_flag,
    CURRENT_TIMESTAMP AS _transformed_at
FROM forecast f
LEFT JOIN attainment a
  ON f.metric_month = a.metric_month
 AND f.segment = a.segment
 AND f.region = a.region
LEFT JOIN budget b
  ON f.metric_month = b.metric_month;

CREATE INDEX idx_metrics_financeops_monthly_month ON analytics.metrics_financeops_monthly(metric_month);
