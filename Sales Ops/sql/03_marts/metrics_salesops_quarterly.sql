-- sql/03_marts/metrics_salesops_quarterly.sql
-- Description: Quarterly KPI aggregates for Sales Operations

DROP TABLE IF EXISTS analytics.metrics_salesops_quarterly;

CREATE TABLE analytics.metrics_salesops_quarterly AS
SELECT
    DATE_TRUNC('quarter', metric_date)::DATE AS metric_quarter,
    EXTRACT(YEAR FROM metric_date)::INTEGER AS metric_year,
    EXTRACT(QUARTER FROM metric_date)::INTEGER AS metric_quarter_number,
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
    CASE WHEN SUM(opp_created) >= 180 THEN 1 ELSE 0 END AS hit_quarterly_creation_goal,
    CASE WHEN SUM(rfp_met_sla_count)::NUMERIC / NULLIF(SUM(rfp_intake_count), 0) >= 0.95 THEN 1 ELSE 0 END AS world_class_rfp_sla_flag,

    -- KPI calculations
    SUM(opp_won)::NUMERIC / NULLIF(SUM(opp_created), 0) AS quarterly_win_rate,
    SUM(won_amount)::NUMERIC / NULLIF(SUM(open_pipeline_amount), 0) AS quarterly_pipeline_conversion_value_rate,
    SUM(rfp_won_count)::NUMERIC / NULLIF(SUM(rfp_submitted_count), 0) AS quarterly_rfp_win_rate,

    -- Metadata
    MAX(_loaded_at) AS _loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM analytics.metrics_salesops_daily
GROUP BY 1, 2, 3, 4, 5;

CREATE INDEX idx_metrics_salesops_quarterly_quarter ON analytics.metrics_salesops_quarterly(metric_quarter);
CREATE INDEX idx_metrics_salesops_quarterly_owner ON analytics.metrics_salesops_quarterly(owner_id);
CREATE INDEX idx_metrics_salesops_quarterly_segment ON analytics.metrics_salesops_quarterly(segment);
