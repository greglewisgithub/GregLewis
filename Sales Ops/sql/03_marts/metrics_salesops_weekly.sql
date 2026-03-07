-- sql/03_marts/metrics_salesops_weekly.sql
-- Description: Weekly KPI aggregates for Sales Operations

DROP TABLE IF EXISTS analytics.metrics_salesops_weekly;

CREATE TABLE analytics.metrics_salesops_weekly AS
SELECT
    DATE_TRUNC('week', metric_date)::DATE AS metric_week,
    segment,
    owner_id,
    SUM(opp_created) AS opp_created,
    SUM(opp_won) AS opp_won,
    SUM(open_pipeline_amount) AS open_pipeline_amount,
    SUM(won_amount) AS won_amount,
    SUM(rfp_intake_count) AS rfp_intake_count,
    SUM(rfp_submitted_count) AS rfp_submitted_count,
    SUM(rfp_won_count) AS rfp_won_count,
    SUM(rfp_met_sla_count) AS rfp_met_sla_count,

    -- Business flags
    CASE WHEN SUM(opp_created) >= 15 THEN 1 ELSE 0 END AS hit_weekly_creation_goal,
    CASE WHEN SUM(rfp_met_sla_count) = SUM(rfp_intake_count) AND SUM(rfp_intake_count) > 0 THEN 1 ELSE 0 END AS perfect_weekly_rfp_sla_flag,

    -- KPI calculations
    SUM(opp_won)::NUMERIC / NULLIF(SUM(opp_created), 0) AS weekly_win_rate,
    SUM(rfp_submitted_count)::NUMERIC / NULLIF(SUM(rfp_intake_count), 0) AS weekly_rfp_submit_rate,

    -- Metadata
    MAX(_loaded_at) AS _loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM analytics.metrics_salesops_daily
GROUP BY 1, 2, 3;

CREATE INDEX idx_metrics_salesops_weekly_week ON analytics.metrics_salesops_weekly(metric_week);
CREATE INDEX idx_metrics_salesops_weekly_owner ON analytics.metrics_salesops_weekly(owner_id);
CREATE INDEX idx_metrics_salesops_weekly_segment ON analytics.metrics_salesops_weekly(segment);
