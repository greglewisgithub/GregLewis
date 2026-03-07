-- sql/01_staging/stg_leads.sql
-- Description: Cleans and standardizes raw lead records for funnel analysis
-- Depends on: raw_data.leads_raw

DROP TABLE IF EXISTS staging.stg_leads;

CREATE TABLE staging.stg_leads AS
WITH source AS (
    SELECT *
    FROM raw_data.leads_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '180 days'
),

cleaned AS (
    SELECT
        lead_id,
        account_id,
        owner_id,
        lead_source,
        qualification_status,
        lead_status,
        lifecycle_stage,
        created_at,
        mql_at,
        sal_at,
        sql_at,
        converted_at,
        disqualified_at,
        last_activity_at,

        -- Business flags
        CASE WHEN qualification_status IN ('Qualified', 'Sales Qualified') THEN 1 ELSE 0 END AS is_qualified,
        CASE WHEN converted_at IS NOT NULL THEN 1 ELSE 0 END AS is_converted,
        CASE WHEN disqualified_at IS NOT NULL THEN 1 ELSE 0 END AS is_disqualified,

        -- KPI calculations
        EXTRACT(DAY FROM (COALESCE(converted_at, CURRENT_DATE) - created_at))::INTEGER AS lead_age_days,
        EXTRACT(DAY FROM (COALESCE(sql_at, CURRENT_DATE) - COALESCE(mql_at, created_at)))::INTEGER AS mql_to_sql_days,

        -- Metadata
        load_date AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM source
    WHERE lead_id IS NOT NULL
)

SELECT * FROM cleaned;

CREATE INDEX idx_stg_leads_lead_id ON staging.stg_leads(lead_id);
CREATE INDEX idx_stg_leads_owner_id ON staging.stg_leads(owner_id);
CREATE INDEX idx_stg_leads_created_at ON staging.stg_leads(created_at);
CREATE INDEX idx_stg_leads_stage ON staging.stg_leads(lifecycle_stage);
