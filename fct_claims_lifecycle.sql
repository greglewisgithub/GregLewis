-- sql/03_marts/fct_claims_lifecycle.sql
-- Description: Main fact table for claims analysis
-- Depends on: intermediate.int_claims_enriched
-- JIRA: RCM-301

DROP TABLE IF EXISTS analytics.fct_claims_lifecycle;

CREATE TABLE analytics.fct_claims_lifecycle AS
WITH claims AS (
    SELECT * FROM intermediate.int_claims_enriched
)

SELECT
    claim_id,
    patient_id,
    payer_id,
    payer_name,
    payer_type,
    rendering_provider_id,
    provider_name,
    specialty,
    
    -- Dates
    claim_created_date,
    claim_submitted_date,
    service_date,
    first_payment_date,
    last_payment_date,
    
    -- Financial metrics
    total_charge_amount,
    total_allowed_amount,
    total_paid_amount,
    actual_payments_received,
    denied_amount,
    
    -- Performance metrics
    denial_count,
    overturn_count,
    rework_hours,
    payment_count,
    days_to_payment,
    days_to_submission,
    current_ar_age,
    
    -- Flags
    is_denied,
    is_clean_claim,
    is_first_pass_paid,
    
    -- Categorization
    claim_status,
    claim_type,
    place_of_service,
    aging_bucket,
    denial_types,
    latest_denial_status,
    
    -- Calculated KPIs
    CASE 
        WHEN total_allowed_amount > 0 
        THEN (actual_payments_received / total_allowed_amount) 
        ELSE 0 
    END AS net_collection_rate,
    
    CASE 
        WHEN total_charge_amount > 0 
        THEN (denied_amount / total_charge_amount) 
        ELSE 0 
    END AS denial_rate_pct,
    
    CASE 
        WHEN total_allowed_amount > 0 
        THEN ((total_allowed_amount - denied_amount) / total_allowed_amount)
        ELSE 0
    END AS expected_collection_rate,
    
    -- Metadata
    _loaded_at,
    _transformed_at,
    CURRENT_TIMESTAMP AS _mart_created_at

FROM claims;

-- Indexes for Tableau performance
CREATE INDEX idx_fct_claims_claim_id ON analytics.fct_claims_lifecycle(claim_id);
CREATE INDEX idx_fct_claims_submit_date ON analytics.fct_claims_lifecycle(claim_submitted_date);
CREATE INDEX idx_fct_claims_payer ON analytics.fct_claims_lifecycle(payer_id);
CREATE INDEX idx_fct_claims_provider ON analytics.fct_claims_lifecycle(rendering_provider_id);
CREATE INDEX idx_fct_claims_aging ON analytics.fct_claims_lifecycle(aging_bucket);
CREATE INDEX idx_fct_claims_clean ON analytics.fct_claims_lifecycle(is_clean_claim);

-- Composite indexes for common query patterns
CREATE INDEX idx_fct_claims_date_payer ON analytics.fct_claims_lifecycle(claim_submitted_date, payer_id);
CREATE INDEX idx_fct_claims_date_status ON analytics.fct_claims_lifecycle(claim_submitted_date, claim_status);

-- Table statistics for query planner
ANALYZE analytics.fct_claims_lifecycle;

-- Monitoring
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date)
SELECT 'fct_claims_lifecycle', COUNT(*), CURRENT_DATE FROM analytics.fct_claims_lifecycle;

-- Confluence documentation: https://synapticure.atlassian.net/wiki/spaces/DATA/pages/fct_claims_lifecycle
