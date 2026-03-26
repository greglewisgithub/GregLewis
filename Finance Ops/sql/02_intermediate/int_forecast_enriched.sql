-- sql/02_intermediate/int_forecast_enriched.sql
-- Description: Forecast snapshots enriched with period-relative metrics and blend logic

DROP TABLE IF EXISTS intermediate.int_forecast_enriched;

CREATE TABLE intermediate.int_forecast_enriched AS
WITH base AS (
    SELECT
        snapshot_id,
        snapshot_date,
        forecast_month,
        owner_id,
        segment,
        region,
        forecast_category,
        committed_amount,
        best_case_amount,
        pipeline_amount,
        _loaded_at,
        DATE_PART('day', forecast_month - snapshot_date) AS days_to_period_start
    FROM staging.stg_forecast_snapshots
)
SELECT
    snapshot_id,
    snapshot_date,
    forecast_month,
    owner_id,
    segment,
    region,
    forecast_category,
    committed_amount,
    best_case_amount,
    pipeline_amount,
    days_to_period_start,
    -- Weighted blend for executive planning
    (committed_amount + (best_case_amount * 0.6) + (pipeline_amount * 0.2))::NUMERIC(18,2) AS blended_forecast_amount,
    _loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM base;

CREATE INDEX idx_int_forecast_enriched_month_owner ON intermediate.int_forecast_enriched(forecast_month, owner_id);
