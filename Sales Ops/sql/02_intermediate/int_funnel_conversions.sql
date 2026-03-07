-- sql/02_intermediate/int_funnel_conversions.sql
-- Description: Builds stage conversion and velocity metrics across lead and opportunity funnel

DROP TABLE IF EXISTS intermediate.int_funnel_conversions;

CREATE TABLE intermediate.int_funnel_conversions AS
WITH leads AS (
    SELECT * FROM staging.stg_leads
),
opps AS (
    SELECT * FROM staging.stg_opportunities
),
joined AS (
    SELECT
        l.lead_id,
        l.account_id,
        l.owner_id,
        l.created_at AS lead_created_at,
        l.mql_at,
        l.sql_at,
        l.converted_at,
        o.opportunity_id,
        o.created_at AS opp_created_at,
        o.close_date,
        o.stage_name,
        o.amount,

        -- Business flags
        l.is_qualified,
        l.is_converted,
        CASE WHEN o.opportunity_id IS NOT NULL THEN 1 ELSE 0 END AS became_opportunity,
        CASE WHEN o.stage_name = 'Closed Won' THEN 1 ELSE 0 END AS became_customer,

        -- KPI calculations
        EXTRACT(DAY FROM (COALESCE(l.mql_at, CURRENT_DATE) - l.created_at))::INTEGER AS days_lead_to_mql,
        EXTRACT(DAY FROM (COALESCE(l.sql_at, CURRENT_DATE) - COALESCE(l.mql_at, l.created_at)))::INTEGER AS days_mql_to_sql,
        EXTRACT(DAY FROM (COALESCE(o.created_at, CURRENT_DATE) - COALESCE(l.sql_at, l.created_at)))::INTEGER AS days_sql_to_opp,
        EXTRACT(DAY FROM (COALESCE(o.close_date, CURRENT_DATE) - COALESCE(o.created_at, l.created_at)))::INTEGER AS days_opp_to_close,

        -- Metadata
        GREATEST(l._loaded_at, COALESCE(o._loaded_at, l._loaded_at)) AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM leads l
    LEFT JOIN opps o ON l.account_id = o.account_id AND l.owner_id = o.owner_id
)
SELECT * FROM joined;

CREATE INDEX idx_int_funnel_lead_id ON intermediate.int_funnel_conversions(lead_id);
CREATE INDEX idx_int_funnel_opp_id ON intermediate.int_funnel_conversions(opportunity_id);
CREATE INDEX idx_int_funnel_owner ON intermediate.int_funnel_conversions(owner_id);
CREATE INDEX idx_int_funnel_stage ON intermediate.int_funnel_conversions(stage_name);
