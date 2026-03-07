-- sql/02_intermediate/int_pipeline_enriched.sql
-- Description: Enriches opportunity records with account traits, rep assignments, and activity rollups

DROP TABLE IF EXISTS intermediate.int_pipeline_enriched;

CREATE TABLE intermediate.int_pipeline_enriched AS
WITH opps AS (
    SELECT * FROM staging.stg_opportunities
),
accounts AS (
    SELECT * FROM staging.stg_accounts
),
activity_rollup AS (
    SELECT
        opportunity_id,
        rep_id,
        COUNT(*) AS total_activities,
        SUM(is_call) AS call_count,
        SUM(is_email) AS email_count,
        SUM(is_meeting) AS meeting_count,
        SUM(is_effective_touch) AS effective_touch_count,
        SUM(productive_minutes) AS productive_minutes,
        MAX(activity_date) AS last_activity_date
    FROM staging.stg_activities
    GROUP BY 1, 2
),
enriched AS (
    SELECT
        o.opportunity_id,
        o.account_id,
        o.owner_id,
        COALESCE(ar.rep_id, o.owner_id) AS activity_rep_id,
        o.segment,
        o.stage_name,
        o.forecast_category,
        o.amount,
        o.created_at,
        o.close_date,
        a.industry,
        a.territory,
        a.arr_band,
        a.region_rollup,
        COALESCE(ar.total_activities, 0) AS total_activities,
        COALESCE(ar.call_count, 0) AS call_count,
        COALESCE(ar.email_count, 0) AS email_count,
        COALESCE(ar.meeting_count, 0) AS meeting_count,
        COALESCE(ar.effective_touch_count, 0) AS effective_touch_count,
        COALESCE(ar.productive_minutes, 0) AS productive_minutes,
        ar.last_activity_date,

        -- Business flags
        o.is_closed_won,
        o.is_closed_lost,
        o.is_open_pipeline,
        CASE WHEN COALESCE(ar.total_activities, 0) >= 5 THEN 1 ELSE 0 END AS has_minimum_activity,

        -- KPI calculations
        o.amount / NULLIF(COALESCE(ar.total_activities, 0), 0) AS amount_per_activity,
        EXTRACT(DAY FROM (CURRENT_DATE - COALESCE(ar.last_activity_date, o.created_at)))::INTEGER AS inactivity_days,

        -- Metadata
        GREATEST(o._loaded_at, COALESCE(ar.last_activity_date::timestamp, o._loaded_at)) AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM opps o
    LEFT JOIN accounts a ON o.account_id = a.account_id
    LEFT JOIN activity_rollup ar ON o.opportunity_id = ar.opportunity_id
)
SELECT * FROM enriched;

CREATE INDEX idx_int_pipeline_opp_id ON intermediate.int_pipeline_enriched(opportunity_id);
CREATE INDEX idx_int_pipeline_owner_id ON intermediate.int_pipeline_enriched(owner_id);
CREATE INDEX idx_int_pipeline_stage ON intermediate.int_pipeline_enriched(stage_name);
CREATE INDEX idx_int_pipeline_close_date ON intermediate.int_pipeline_enriched(close_date);
