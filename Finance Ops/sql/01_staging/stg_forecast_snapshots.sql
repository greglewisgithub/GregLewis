-- sql/01_staging/stg_forecast_snapshots.sql
-- Description: Forecast snapshots at owner/period grain

DROP TABLE IF EXISTS staging.stg_forecast_snapshots;

CREATE TABLE staging.stg_forecast_snapshots AS
SELECT
    snapshot_id,
    snapshot_date::DATE AS snapshot_date,
    forecast_month::DATE AS forecast_month,
    owner_id,
    segment,
    region,
    forecast_category,
    committed_amount::NUMERIC(18,2) AS committed_amount,
    best_case_amount::NUMERIC(18,2) AS best_case_amount,
    pipeline_amount::NUMERIC(18,2) AS pipeline_amount,
    source_system,
    created_at::TIMESTAMP AS _loaded_at
FROM raw.forecast_snapshots;

CREATE INDEX idx_stg_forecast_snapshot_date ON staging.stg_forecast_snapshots(snapshot_date);
CREATE INDEX idx_stg_forecast_month ON staging.stg_forecast_snapshots(forecast_month);
