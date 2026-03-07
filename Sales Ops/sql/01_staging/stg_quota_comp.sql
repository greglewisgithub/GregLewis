-- sql/01_staging/stg_quota_comp.sql
-- Description: Cleans quota and compensation plan period data at rep grain
-- Depends on: raw_data.quota_comp_raw

DROP TABLE IF EXISTS staging.stg_quota_comp;

CREATE TABLE staging.stg_quota_comp AS
WITH source AS (
    SELECT *
    FROM raw_data.quota_comp_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '540 days'
),

cleaned AS (
    SELECT
        rep_id,
        plan_period_start,
        plan_period_end,
        quota_amount,
        attainment_amount,
        variable_comp_earned,

        -- Business flags
        CASE WHEN attainment_amount >= quota_amount THEN 1 ELSE 0 END AS is_quota_attained,
        CASE WHEN attainment_amount >= quota_amount * 1.2 THEN 1 ELSE 0 END AS is_over_attainment,

        -- KPI calculations
        attainment_amount / NULLIF(quota_amount, 0) AS attainment_pct,
        variable_comp_earned / NULLIF(attainment_amount, 0) AS comp_per_dollar_booked,

        -- Metadata
        load_date AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM source
    WHERE rep_id IS NOT NULL
)

SELECT * FROM cleaned;

CREATE INDEX idx_stg_quota_rep_id ON staging.stg_quota_comp(rep_id);
CREATE INDEX idx_stg_quota_period_start ON staging.stg_quota_comp(plan_period_start);
CREATE INDEX idx_stg_quota_period_end ON staging.stg_quota_comp(plan_period_end);
