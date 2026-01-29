-- sql/03_marts/fct_denial_analytics.sql
-- Description: Detailed denial fact table for root cause analysis
-- Depends on: intermediate.int_denial_categorization
-- JIRA: RCM-302

DROP TABLE IF EXISTS analytics.fct_denial_analytics;

CREATE TABLE analytics.fct_denial_analytics AS
SELECT
    denial_id,
    claim_id,
    denial_date,
    denial_reason_code,
    denial_reason_description,
    denial_type_grouped,
    denied_amount,
    denial_status,
    appeal_date,
    resolution_date,
    resolution_amount,
    rework_hours,
    is_overturned,
    is_open,
    is_written_off,
    is_aged_open_denial,
    is_at_risk_writeoff,
    
    -- Context
    payer_name,
    payer_type,
    provider_name,
    specialty,
    claim_type,
    service_date,
    total_charge_amount,
    
    -- Categorization
    preventability_category,
    financial_impact_tier,
    
    -- Metrics
    days_to_resolution,
    recovery_rate,
    recovery_per_hour,
    
    -- Metadata
    _loaded_at,
    _transformed_at,
    CURRENT_TIMESTAMP AS _mart_created_at
    
FROM intermediate.int_denial_categorization;

-- Indexes
CREATE INDEX idx_fct_denials_denial_id ON analytics.fct_denial_analytics(denial_id);
CREATE INDEX idx_fct_denials_claim_id ON analytics.fct_denial_analytics(claim_id);
CREATE INDEX idx_fct_denials_date ON analytics.fct_denial_analytics(denial_date);
CREATE INDEX idx_fct_denials_type ON analytics.fct_denial_analytics(denial_type_grouped);
CREATE INDEX idx_fct_denials_reason ON analytics.fct_denial_analytics(denial_reason_code);
CREATE INDEX idx_fct_denials_status ON analytics.fct_denial_analytics(denial_status);

-- Composite indexes
CREATE INDEX idx_fct_denials_date_type ON analytics.fct_denial_analytics(denial_date, denial_type_grouped);
CREATE INDEX idx_fct_denials_payer_type ON analytics.fct_denial_analytics(payer_name, denial_type_grouped);

-- Statistics
ANALYZE analytics.fct_denial_analytics;

-- Monitoring
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date)
SELECT 'fct_denial_analytics', COUNT(*), CURRENT_DATE FROM analytics.fct_denial_analytics;
