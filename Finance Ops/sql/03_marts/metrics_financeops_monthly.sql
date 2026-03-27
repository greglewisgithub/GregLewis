-- sql/03_marts/metrics_financeops_monthly.sql
-- Description: Monthly executive KPI aggregates for Finance Ops

DROP TABLE IF EXISTS analytics.metrics_financeops_monthly;

CREATE TABLE analytics.metrics_financeops_monthly AS
WITH forecast AS (
    SELECT
        forecast_month AS metric_month,
        owner_id,
        segment,
        region,
        SUM(blended_forecast_amount) AS blended_forecast_amount,
        SUM(realized_bookings) AS realized_bookings,
        AVG(forecast_accuracy_ratio) AS avg_forecast_accuracy_ratio
    FROM analytics.fct_revenue_forecast_lifecycle
    GROUP BY 1, 2, 3, 4
),
attainment AS (
    SELECT
        quota_month AS metric_month,
        owner_id,
        segment,
        region,
        SUM(quota_amount) AS total_quota,
        SUM(bookings_amount) AS total_bookings,
        AVG(attainment_rate) AS avg_attainment_rate,
        SUM(hit_quota_flag) AS reps_hitting_quota
    FROM analytics.fct_bookings_attainment
    GROUP BY 1, 2, 3, 4
),
budget AS (
    SELECT
        fiscal_month AS metric_month,
        SUM(planned_amount) AS total_budget,
        SUM(actual_amount) AS total_actuals,
        SUM(variance_amount) AS total_variance
    FROM analytics.fct_budget_actual_variance
    GROUP BY 1
),
arr_mrr AS (
    SELECT
        metric_month,
        owner_id,
        segment,
        region,
        SUM(net_arr_change) AS net_arr_change,
        SUM(net_mrr_change) AS net_mrr_change
    FROM analytics.fct_arr_mrr_movement
    GROUP BY 1, 2, 3, 4
),
leakage AS (
    SELECT
        metric_month,
        owner_id,
        segment,
        region,
        SUM(total_leakage_exposure) AS total_leakage_exposure,
        AVG(avg_days_to_cash) AS avg_days_to_cash,
        AVG(total_leakage_exposure_rate) AS avg_leakage_rate
    FROM analytics.fct_revenue_leakage
    GROUP BY 1, 2, 3, 4
)
SELECT
    f.metric_month,
    f.owner_id,
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
    COALESCE(am.net_arr_change, 0) AS net_arr_change,
    COALESCE(am.net_mrr_change, 0) AS net_mrr_change,
    COALESCE(l.total_leakage_exposure, 0) AS total_leakage_exposure,
    COALESCE(l.avg_days_to_cash, 0) AS avg_days_to_cash,
    COALESCE(l.avg_leakage_rate, 0) AS avg_leakage_rate,
    CASE WHEN f.avg_forecast_accuracy_ratio >= 0.95 THEN 1 ELSE 0 END AS on_target_forecast_flag,
    CASE WHEN COALESCE(a.avg_attainment_rate, 0) >= 1 THEN 1 ELSE 0 END AS on_target_attainment_flag,
    CURRENT_TIMESTAMP AS _transformed_at
FROM forecast f
LEFT JOIN attainment a
  ON f.metric_month = a.metric_month
 AND f.owner_id = a.owner_id
 AND f.segment = a.segment
 AND f.region = a.region
LEFT JOIN budget b
  ON f.metric_month = b.metric_month
LEFT JOIN arr_mrr am
  ON f.metric_month = am.metric_month
 AND f.owner_id = am.owner_id
 AND f.segment = am.segment
 AND f.region = am.region
LEFT JOIN leakage l
  ON f.metric_month = l.metric_month
 AND f.owner_id = l.owner_id
 AND f.segment = l.segment
 AND f.region = l.region;

CREATE INDEX idx_metrics_financeops_monthly_month ON analytics.metrics_financeops_monthly(metric_month);
