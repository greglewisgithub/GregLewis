-- sql/03_marts/metrics_salesops_daily.sql
-- Description: Daily KPI aggregates for Sales Operations

DROP TABLE IF EXISTS analytics.metrics_salesops_daily;

CREATE TABLE analytics.metrics_salesops_daily AS
WITH pipeline AS (
    SELECT
        DATE_TRUNC('day', created_at)::DATE AS metric_date,
        segment,
        owner_id,
        COUNT(*) AS opp_created,
        SUM(is_closed_won) AS opp_won,
        SUM(is_open_pipeline) AS opp_open,
        SUM(open_pipeline_amount) AS open_pipeline_amount,
        SUM(closed_won_amount) AS won_amount,
        AVG(days_sql_to_opp) AS avg_days_sql_to_opp,
        AVG(activity_quality_ratio) AS avg_activity_quality_ratio
    FROM analytics.fct_sales_pipeline_lifecycle
    GROUP BY 1, 2, 3
),
rfp AS (
    SELECT
        DATE_TRUNC('day', intake_date)::DATE AS metric_date,
        owner_id,
        COUNT(*) AS rfp_intake_count,
        SUM(is_submitted) AS rfp_submitted_count,
        SUM(is_won) AS rfp_won_count,
        SUM(met_sla) AS rfp_met_sla_count
    FROM analytics.fct_rfp_operations
    GROUP BY 1, 2
)
SELECT
    p.metric_date,
    p.segment,
    p.owner_id,
    p.opp_created,
    p.opp_won,
    p.opp_open,
    p.open_pipeline_amount,
    p.won_amount,
    p.avg_days_sql_to_opp,
    p.avg_activity_quality_ratio,
    COALESCE(r.rfp_intake_count, 0) AS rfp_intake_count,
    COALESCE(r.rfp_submitted_count, 0) AS rfp_submitted_count,
    COALESCE(r.rfp_won_count, 0) AS rfp_won_count,
    COALESCE(r.rfp_met_sla_count, 0) AS rfp_met_sla_count,

    -- Business flags
    CASE WHEN p.opp_created >= 3 THEN 1 ELSE 0 END AS hit_daily_opp_creation_flag,
    CASE WHEN COALESCE(r.rfp_met_sla_count, 0) = COALESCE(r.rfp_intake_count, 0) AND COALESCE(r.rfp_intake_count, 0) > 0 THEN 1 ELSE 0 END AS perfect_rfp_sla_day_flag,

    -- KPI calculations
    p.opp_won::NUMERIC / NULLIF(p.opp_created, 0) AS daily_opp_win_rate,
    COALESCE(r.rfp_submitted_count, 0)::NUMERIC / NULLIF(COALESCE(r.rfp_intake_count, 0), 0) AS daily_rfp_submit_rate,

    -- Metadata
    p.metric_date::timestamp AS _loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM pipeline p
LEFT JOIN rfp r ON p.metric_date = r.metric_date AND p.owner_id = r.owner_id;

CREATE INDEX idx_metrics_salesops_daily_date ON analytics.metrics_salesops_daily(metric_date);
CREATE INDEX idx_metrics_salesops_daily_owner ON analytics.metrics_salesops_daily(owner_id);
CREATE INDEX idx_metrics_salesops_daily_segment ON analytics.metrics_salesops_daily(segment);
