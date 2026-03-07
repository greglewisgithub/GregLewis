-- sql/01_staging/stg_opportunities.sql
-- Description: Cleans and standardizes opportunity data for pipeline reporting
-- Depends on: raw_data.opportunities_raw

DROP TABLE IF EXISTS staging.stg_opportunities;

CREATE TABLE staging.stg_opportunities AS
WITH source AS (
    SELECT *
    FROM raw_data.opportunities_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '365 days'
),

cleaned AS (
    SELECT
        opportunity_id,
        account_id,
        owner_id,
        segment,
        stage_name,
        forecast_category,
        amount,
        created_at,
        close_date,
        won_at,
        lost_at,
        pipeline_source,

        -- Business flags
        CASE WHEN stage_name IN ('Closed Won') THEN 1 ELSE 0 END AS is_closed_won,
        CASE WHEN stage_name IN ('Closed Lost') THEN 1 ELSE 0 END AS is_closed_lost,
        CASE WHEN stage_name NOT IN ('Closed Won', 'Closed Lost') THEN 1 ELSE 0 END AS is_open_pipeline,

        -- KPI calculations
        EXTRACT(DAY FROM (COALESCE(close_date, CURRENT_DATE) - created_at))::INTEGER AS cycle_days,
        CASE WHEN amount >= 100000 THEN 'Enterprise' WHEN amount >= 25000 THEN 'Mid-Market' ELSE 'SMB' END AS deal_size_band,

        -- Metadata
        load_date AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM source
    WHERE opportunity_id IS NOT NULL
)

SELECT * FROM cleaned;

CREATE INDEX idx_stg_opps_opp_id ON staging.stg_opportunities(opportunity_id);
CREATE INDEX idx_stg_opps_account_id ON staging.stg_opportunities(account_id);
CREATE INDEX idx_stg_opps_owner_id ON staging.stg_opportunities(owner_id);
CREATE INDEX idx_stg_opps_close_date ON staging.stg_opportunities(close_date);
CREATE INDEX idx_stg_opps_stage_name ON staging.stg_opportunities(stage_name);
