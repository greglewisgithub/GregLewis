-- sql/02_intermediate/int_denial_categorization.sql
-- Description: Deep denial analysis with preventability scoring
-- Depends on: staging.stg_denials, intermediate.int_claims_enriched
-- JIRA: RCM-202

DROP TABLE IF EXISTS intermediate.int_denial_categorization;

CREATE TABLE intermediate.int_denial_categorization AS
WITH denials AS (
    SELECT * FROM staging.stg_denials
),

claims AS (
    SELECT * FROM intermediate.int_claims_enriched
),

denial_analysis AS (
    SELECT
        d.denial_id,
        d.claim_id,
        d.denial_date,
        d.denial_reason_code,
        d.denial_reason_description,
        d.denial_type_grouped,
        d.denied_amount,
        d.denial_status,
        d.appeal_date,
        d.resolution_date,
        d.resolution_amount,
        d.rework_hours,
        d.is_overturned,
        d.is_open,
        d.is_written_off,
        
        -- Claim context
        c.payer_name,
        c.payer_type,
        c.provider_name,
        c.specialty,
        c.claim_type,
        c.service_date,
        c.total_charge_amount,
        
        -- Preventability classification
        CASE
            WHEN d.denial_type_grouped IN ('Authorization', 'Eligibility') 
                THEN 'Highly Preventable'
            WHEN d.denial_type_grouped IN ('Documentation', 'Coding') 
                THEN 'Preventable'
            WHEN d.denial_type_grouped IN ('Timely Filing')
                THEN 'Process Failure'
            WHEN d.denial_type_grouped = 'Other' 
                THEN 'Review Required'
        END AS preventability_category,
        
        -- Financial impact tier
        CASE
            WHEN d.denied_amount >= 5000 THEN 'High Impact'
            WHEN d.denied_amount >= 1000 THEN 'Medium Impact'
            ELSE 'Low Impact'
        END AS financial_impact_tier,
        
        -- Resolution metrics
        EXTRACT(DAY FROM (COALESCE(d.resolution_date, CURRENT_DATE) - d.denial_date))::INTEGER 
            AS days_to_resolution,
        
        CASE 
            WHEN d.denial_status = 'Overturned' AND d.denied_amount > 0
                THEN (d.resolution_amount / NULLIF(d.denied_amount, 0))
            ELSE 0
        END AS recovery_rate,
        
        -- Time-based flags
        CASE 
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - d.denial_date)) > 90 
                AND d.denial_status = 'Open' 
            THEN 1 
            ELSE 0 
        END AS is_aged_open_denial,
        
        CASE 
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - d.denial_date)) > 120 
                AND d.denial_status = 'Open' 
            THEN 1 
            ELSE 0 
        END AS is_at_risk_writeoff,
        
        -- Efficiency metrics
        CASE 
            WHEN d.rework_hours > 0 
                THEN (d.resolution_amount / NULLIF(d.rework_hours, 0))
            ELSE 0
        END AS recovery_per_hour,
        
        -- Metadata
        d._loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
        
    FROM denials d
    LEFT JOIN claims c ON d.claim_id = c.claim_id
)

--SELECT * FROM denial_analysis;

-- Indexes
CREATE INDEX idx_int_denials_denial_id ON intermediate.int_denial_categorization(denial_id);
CREATE INDEX idx_int_denials_claim_id ON intermediate.int_denial_categorization(claim_id);
CREATE INDEX idx_int_denials_date ON intermediate.int_denial_categorization(denial_date);
CREATE INDEX idx_int_denials_type ON intermediate.int_denial_categorization(denial_type_grouped);
CREATE INDEX idx_int_denials_status ON intermediate.int_denial_categorization(denial_status);
CREATE INDEX idx_int_denials_preventability ON intermediate.int_denial_categorization(preventability_category);

-- Monitoring
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date)
SELECT 'int_denial_categorization', COUNT(*), CURRENT_DATE FROM intermediate.int_denial_categorization;
