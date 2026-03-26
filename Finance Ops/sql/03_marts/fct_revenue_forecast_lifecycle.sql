-- sql/03_marts/fct_revenue_forecast_lifecycle.sql
-- Description: Forecast lifecycle fact table with snapshots and realized performance

DROP TABLE IF EXISTS analytics.fct_revenue_forecast_lifecycle;

CREATE TABLE analytics.fct_revenue_forecast_lifecycle AS
WITH forecast AS (
    SELECT * FROM intermediate.int_forecast_enriched
),
actuals AS (
    SELECT
        owner_id,
        DATE_TRUNC('month', booking_date)::DATE AS forecast_month,
        SUM(booking_amount) AS realized_bookings
    FROM staging.stg_bookings
    GROUP BY 1, 2
)
SELECT
    f.snapshot_id,
    f.snapshot_date,
    f.forecast_month,
    f.owner_id,
    f.segment,
    f.region,
    f.forecast_category,
    f.committed_amount,
    f.best_case_amount,
    f.pipeline_amount,
    f.blended_forecast_amount,
    COALESCE(a.realized_bookings, 0) AS realized_bookings,
    COALESCE(a.realized_bookings, 0) - f.blended_forecast_amount AS forecast_variance_amount,
    COALESCE(a.realized_bookings, 0)::NUMERIC / NULLIF(f.blended_forecast_amount, 0) AS forecast_accuracy_ratio,
    f.days_to_period_start,
    f._loaded_at,
    f._transformed_at,
    CURRENT_TIMESTAMP AS _mart_created_at
FROM forecast f
LEFT JOIN actuals a
  ON f.owner_id = a.owner_id
 AND f.forecast_month = a.forecast_month;

CREATE INDEX idx_fct_revenue_forecast_month ON analytics.fct_revenue_forecast_lifecycle(forecast_month);
CREATE INDEX idx_fct_revenue_forecast_owner ON analytics.fct_revenue_forecast_lifecycle(owner_id);
