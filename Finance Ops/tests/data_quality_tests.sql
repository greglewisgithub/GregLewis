-- Finance Ops data quality tests

-- 1) Ensure no duplicate snapshot rows per owner/month/date
SELECT owner_id, forecast_month, snapshot_date, COUNT(*) AS row_count
FROM analytics.fct_revenue_forecast_lifecycle
GROUP BY owner_id, forecast_month, snapshot_date
HAVING COUNT(*) > 1;

-- 2) Ensure quota values are non-negative
SELECT owner_id, quota_month, quota_amount
FROM analytics.fct_bookings_attainment
WHERE quota_amount < 0;

-- 3) Ensure forecast accuracy ratio is not negative
SELECT owner_id, forecast_month, forecast_accuracy_ratio
FROM analytics.fct_revenue_forecast_lifecycle
WHERE forecast_accuracy_ratio < 0;

-- 4) Ensure total leakage exposure is non-negative
SELECT owner_id, metric_month, total_leakage_exposure
FROM analytics.fct_revenue_leakage
WHERE total_leakage_exposure < 0;

-- 5) Ensure movement type values are in the defined set
SELECT movement_type, COUNT(*) AS bad_rows
FROM analytics.fct_arr_mrr_movement
WHERE movement_type NOT IN ('New', 'Expansion', 'Contraction', 'Churn', 'Flat')
GROUP BY movement_type;
