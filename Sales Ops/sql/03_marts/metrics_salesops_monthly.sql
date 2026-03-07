-- sql/03_marts/metrics_salesops_monthly.sql
-- Description: Monthly KPI aggregates for Sales Operations

DROP TABLE IF EXISTS analytics.metrics_salesops_monthly;

CREATE TABLE analytics.metrics_salesops_monthly AS
SELECT
    DATE_TRUNC('month', metric_date)::DATE AS metric_month,
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
    CASE WHEN SUM(opp_created) >= 60 THEN 1 ELSE 0 END AS hit_monthly_creation_goal,
    CASE WHEN SUM(won_amount) >= 250000 THEN 1 ELSE 0 END AS hit_monthly_bookings_goal,

    -- KPI calculations
    SUM(opp_won)::NUMERIC / NULLIF(SUM(opp_created), 0) AS monthly_win_rate,
    SUM(won_amount)::NUMERIC / NULLIF(SUM(open_pipeline_amount), 0) AS monthly_pipeline_conversion_value_rate,
    SUM(rfp_won_count)::NUMERIC / NULLIF(SUM(rfp_submitted_count), 0) AS monthly_rfp_win_rate,

    -- Metadata
    MAX(_loaded_at) AS _loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM analytics.metrics_salesops_daily
GROUP BY 1, 2, 3;

CREATE INDEX idx_metrics_salesops_monthly_month ON analytics.metrics_salesops_monthly(metric_month);
CREATE INDEX idx_metrics_salesops_monthly_owner ON analytics.metrics_salesops_monthly(owner_id);
CREATE INDEX idx_metrics_salesops_monthly_segment ON analytics.metrics_salesops_monthly(segment);
