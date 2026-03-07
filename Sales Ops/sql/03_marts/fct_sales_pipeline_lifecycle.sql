-- sql/03_marts/fct_sales_pipeline_lifecycle.sql
-- Description: Sales pipeline lifecycle fact table at opportunity grain

DROP TABLE IF EXISTS analytics.fct_sales_pipeline_lifecycle;

CREATE TABLE analytics.fct_sales_pipeline_lifecycle AS
WITH pipeline AS (
    SELECT * FROM intermediate.int_pipeline_enriched
),
funnel AS (
    SELECT opportunity_id, MAX(days_sql_to_opp) AS days_sql_to_opp
    FROM intermediate.int_funnel_conversions
    GROUP BY 1
)
SELECT
    p.opportunity_id,
    p.account_id,
    p.owner_id,
    p.activity_rep_id,
    p.segment,
    p.stage_name,
    p.forecast_category,
    p.industry,
    p.territory,
    p.arr_band,
    p.region_rollup,
    p.created_at,
    p.close_date,
    p.amount,
    p.total_activities,
    p.effective_touch_count,
    p.productive_minutes,
    p.amount_per_activity,
    p.inactivity_days,
    COALESCE(f.days_sql_to_opp, 0) AS days_sql_to_opp,

    -- Business flags
    p.is_open_pipeline,
    p.is_closed_won,
    p.is_closed_lost,
    p.has_minimum_activity,
    CASE WHEN p.forecast_category IN ('Commit', 'Best Case') THEN 1 ELSE 0 END AS is_in_forecast,

    -- KPI calculations
    CASE WHEN p.is_closed_won = 1 THEN p.amount ELSE 0 END AS closed_won_amount,
    CASE WHEN p.is_open_pipeline = 1 THEN p.amount ELSE 0 END AS open_pipeline_amount,
    p.effective_touch_count::NUMERIC / NULLIF(p.total_activities, 0) AS activity_quality_ratio,

    -- Metadata
    p._loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM pipeline p
LEFT JOIN funnel f ON p.opportunity_id = f.opportunity_id;

CREATE INDEX idx_fct_pipeline_opp_id ON analytics.fct_sales_pipeline_lifecycle(opportunity_id);
CREATE INDEX idx_fct_pipeline_owner_id ON analytics.fct_sales_pipeline_lifecycle(owner_id);
CREATE INDEX idx_fct_pipeline_created_at ON analytics.fct_sales_pipeline_lifecycle(created_at);
CREATE INDEX idx_fct_pipeline_stage ON analytics.fct_sales_pipeline_lifecycle(stage_name);
CREATE INDEX idx_fct_pipeline_forecast ON analytics.fct_sales_pipeline_lifecycle(forecast_category);
