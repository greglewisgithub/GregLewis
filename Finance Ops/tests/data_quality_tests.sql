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
