-- sql/01_staging/stg_activities.sql
-- Description: Standardizes seller activity data across calls, emails, and meetings
-- Depends on: raw_data.activities_raw

DROP TABLE IF EXISTS staging.stg_activities;

CREATE TABLE staging.stg_activities AS
WITH source AS (
    SELECT *
    FROM raw_data.activities_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '180 days'
),

cleaned AS (
    SELECT
        activity_id,
        opportunity_id,
        account_id,
        rep_id,
        activity_type,
        activity_date,
        outcome,
        duration_minutes,

        -- Business flags
        CASE WHEN activity_type = 'Call' THEN 1 ELSE 0 END AS is_call,
        CASE WHEN activity_type = 'Email' THEN 1 ELSE 0 END AS is_email,
        CASE WHEN activity_type = 'Meeting' THEN 1 ELSE 0 END AS is_meeting,
        CASE WHEN outcome IN ('Connected', 'Completed') THEN 1 ELSE 0 END AS is_effective_touch,

        -- KPI calculations
        CASE WHEN duration_minutes > 0 THEN duration_minutes ELSE 0 END AS productive_minutes,

        -- Metadata
        load_date AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM source
    WHERE activity_id IS NOT NULL
)

SELECT * FROM cleaned;

CREATE INDEX idx_stg_activities_activity_id ON staging.stg_activities(activity_id);
CREATE INDEX idx_stg_activities_rep_id ON staging.stg_activities(rep_id);
CREATE INDEX idx_stg_activities_date ON staging.stg_activities(activity_date);
CREATE INDEX idx_stg_activities_opp_id ON staging.stg_activities(opportunity_id);
