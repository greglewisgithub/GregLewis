-- sql/02_intermediate/int_rep_productivity.sql
-- Description: Computes rep productivity and activity efficiency against opportunities and quota

DROP TABLE IF EXISTS intermediate.int_rep_productivity;

CREATE TABLE intermediate.int_rep_productivity AS
WITH activity AS (
    SELECT
        rep_id,
        DATE_TRUNC('month', activity_date)::DATE AS metric_month,
        COUNT(*) AS activity_count,
        SUM(is_effective_touch) AS effective_touch_count,
        SUM(productive_minutes) AS productive_minutes
    FROM staging.stg_activities
    GROUP BY 1, 2
),
opps AS (
    SELECT
        owner_id AS rep_id,
        DATE_TRUNC('month', created_at)::DATE AS metric_month,
        COUNT(*) AS opp_created_count,
        SUM(is_closed_won) AS won_count,
        SUM(amount) AS pipeline_amount,
        SUM(CASE WHEN is_closed_won = 1 THEN amount ELSE 0 END) AS won_amount
    FROM staging.stg_opportunities
    GROUP BY 1, 2
),
quota AS (
    SELECT
        rep_id,
        DATE_TRUNC('month', plan_period_start)::DATE AS metric_month,
        MAX(quota_amount) AS monthly_quota_amount,
        MAX(attainment_amount) AS monthly_attainment_amount,
        MAX(attainment_pct) AS attainment_pct
    FROM staging.stg_quota_comp
    GROUP BY 1, 2
),
joined AS (
    SELECT
        COALESCE(a.rep_id, o.rep_id, q.rep_id) AS rep_id,
        COALESCE(a.metric_month, o.metric_month, q.metric_month) AS metric_month,
        COALESCE(a.activity_count, 0) AS activity_count,
        COALESCE(a.effective_touch_count, 0) AS effective_touch_count,
        COALESCE(a.productive_minutes, 0) AS productive_minutes,
        COALESCE(o.opp_created_count, 0) AS opp_created_count,
        COALESCE(o.won_count, 0) AS won_count,
        COALESCE(o.pipeline_amount, 0) AS pipeline_amount,
        COALESCE(o.won_amount, 0) AS won_amount,
        COALESCE(q.monthly_quota_amount, 0) AS monthly_quota_amount,
        COALESCE(q.monthly_attainment_amount, 0) AS monthly_attainment_amount,
        COALESCE(q.attainment_pct, 0) AS attainment_pct,

        -- Business flags
        CASE WHEN COALESCE(a.activity_count, 0) >= 60 THEN 1 ELSE 0 END AS hit_activity_target,
        CASE WHEN COALESCE(o.won_count, 0) > 0 THEN 1 ELSE 0 END AS has_won_business,
        CASE WHEN COALESCE(q.attainment_pct, 0) >= 1 THEN 1 ELSE 0 END AS hit_quota,

        -- KPI calculations
        COALESCE(o.opp_created_count, 0)::NUMERIC / NULLIF(COALESCE(a.activity_count, 0), 0) AS opps_per_activity,
        COALESCE(o.won_count, 0)::NUMERIC / NULLIF(COALESCE(o.opp_created_count, 0), 0) AS opp_win_rate,
        COALESCE(o.won_amount, 0)::NUMERIC / NULLIF(COALESCE(a.effective_touch_count, 0), 0) AS booked_per_effective_touch,

        -- Metadata
        CURRENT_DATE::timestamp AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM activity a
    FULL OUTER JOIN opps o ON a.rep_id = o.rep_id AND a.metric_month = o.metric_month
    FULL OUTER JOIN quota q ON COALESCE(a.rep_id, o.rep_id) = q.rep_id
        AND COALESCE(a.metric_month, o.metric_month) = q.metric_month
)
SELECT * FROM joined;

CREATE INDEX idx_int_rep_productivity_rep_id ON intermediate.int_rep_productivity(rep_id);
CREATE INDEX idx_int_rep_productivity_month ON intermediate.int_rep_productivity(metric_month);
CREATE INDEX idx_int_rep_productivity_target ON intermediate.int_rep_productivity(hit_activity_target);
