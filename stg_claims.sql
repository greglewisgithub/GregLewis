-- sql/01_staging/stg_claims.sql
-- Description: Cleans and standardizes raw claims data
-- Depends on: raw_data.claims_raw
-- JIRA: RCM-101

DROP TABLE IF EXISTS staging.stg_claims;

CREATE TABLE staging.stg_claims AS
WITH source AS (
    SELECT * FROM raw_data.claims_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '90 days'  -- Incremental load
),

cleaned AS (
    SELECT
        claim_id,
        patient_id,
        payer_id,
        rendering_provider_id,
        billing_provider_id,
        claim_created_date,
        claim_submitted_date,
        DATE(claim_submitted_date) AS service_date,
        claim_status,
        claim_type,
        place_of_service,
        total_charge_amount,
        total_allowed_amount,
        total_paid_amount,
        patient_responsibility,
        
        -- Data quality flags
        CASE 
            WHEN claim_submitted_date IS NULL THEN 1 
            ELSE 0 
        END AS missing_submit_date_flag,
        
        CASE 
            WHEN total_charge_amount <= 0 THEN 1 
            ELSE 0 
        END AS invalid_charge_flag,
        
        -- Metadata
        load_date AS _loaded_at,
        'CLAIMS_RAW' AS _source_system,
        CURRENT_TIMESTAMP AS _transformed_at
        
    FROM source
    WHERE claim_id IS NOT NULL
)

--SELECT * FROM cleaned;

-- Create indexes for performance
CREATE INDEX idx_stg_claims_claim_id ON staging.stg_claims(claim_id);
CREATE INDEX idx_stg_claims_submit_date ON staging.stg_claims(claim_submitted_date);
CREATE INDEX idx_stg_claims_payer ON staging.stg_claims(payer_id);

-- Log row counts for monitoring
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date)
SELECT 'stg_claims', COUNT(*), CURRENT_DATE FROM staging.stg_claims;

-- Confluence documentation: https://synapticure.atlassian.net/wiki/spaces/DATA/pages/stg_claims
