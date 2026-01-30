-- sql/03_marts/metrics_rcm_daily.sql
-- Description: Pre-aggregated daily metrics for dashboard performance
-- Depends on: analytics.fct_claims_lifecycle, analytics.fct_denial_analytics
-- JIRA: RCM-303

DROP TABLE IF EXISTS analytics.metrics_rcm_daily;

CREATE TABLE analytics.metrics_rcm_daily AS
WITH daily_claims AS (
    SELECT
        DATE_TRUNC('day', claim_submitted_date)::DATE AS metric_date,
        payer_type,
        specialty,
        aging_bucket,
        
        -- Volume metrics
        COUNT(*) AS total_claims_submitted,
        SUM(is_clean_claim) AS clean_claims,
        SUM(is_denied) AS denied_claims,
        SUM(is_first_pass_paid) AS first_pass_paid_claims,
        
        -- Financial metrics
        SUM(total_charge_amount) AS total_charges,
        SUM(total_allowed_amount) AS total_allowed,
        SUM(actual_payments_received) AS total_payments,
        SUM(denied_amount) AS total_denied,
        
        -- Efficiency metrics
        AVG(days_to_payment) AS avg_days_to_payment,
        AVG(days_to_submission) AS avg_days_to_submission,
        AVG(current_ar_age) AS avg_ar_age,
        SUM(rework_hours) AS total_rework_hours,
        
        -- Outstanding AR
        SUM(CASE WHEN claim_status != 'Paid' THEN total_allowed_amount - actual_payments_received ELSE 0 END) 
            AS outstanding_ar,
        
        -- Calculated KPIs
        SUM(is_clean_claim)::NUMERIC / NULLIF(COUNT(*), 0) AS clean_claim_rate,
        SUM(is_denied)::NUMERIC / NULLIF(COUNT(*), 0) AS denial_rate,
        SUM(is_first_pass_paid)::NUMERIC / NULLIF(COUNT(*), 0) AS first_pass_resolution_rate,
        SUM(actual_payments_received) / NULLIF(SUM(total_allowed_amount), 0) AS net_collection_rate
        
    FROM analytics.fct_claims_lifecycle
    WHERE claim_submitted_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

daily_denials AS (
    SELECT
        DATE_TRUNC('day', denial_date)::DATE AS metric_date,
        denial_type_grouped,
        preventability_category,
        payer_type,
        
        -- Denial metrics
        COUNT(*) AS denial_count,
        SUM(denied_amount) AS denied_amount,
        AVG(days_to_resolution) AS avg_days_to_resolution,
        SUM(is_overturned) AS overturned_count,
        AVG(recovery_rate) AS avg_recovery_rate,
        SUM(rework_hours) AS denial_rework_hours,
        AVG(recovery_per_hour) AS avg_recovery_per_hour,
        SUM(is_aged_open_denial) AS aged_open_count,
        SUM(is_at_risk_writeoff) AS at_risk_writeoff_count
        
    FROM analytics.fct_denial_analytics
    WHERE denial_date IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    -- Date dimensions
    dc.metric_date,
    EXTRACT(YEAR FROM dc.metric_date)::INTEGER AS metric_year,
    EXTRACT(QUARTER FROM dc.metric_date)::INTEGER AS metric_quarter,
    EXTRACT(MONTH FROM dc.metric_date)::INTEGER AS metric_month,
    EXTRACT(DOW FROM dc.metric_date)::INTEGER AS day_of_week,
    TO_CHAR(dc.metric_date, 'Month') AS month_name,
    TO_CHAR(dc.metric_date, 'Day') AS day_name,
    
    -- Grouping dimensions
    dc.payer_type,
    dc.specialty,
    dc.aging_bucket,
    dd.denial_type_grouped,
    dd.preventability_category,
    
    -- Claims metrics
    dc.total_claims_submitted,
    dc.clean_claims,
    dc.denied_claims,
    dc.first_pass_paid_claims,
    dc.total_charges,
    dc.total_allowed,
    dc.total_payments,
    dc.total_denied,
    dc.outstanding_ar,
    dc.avg_days_to_payment,
    dc.avg_days_to_submission,
    dc.avg_ar_age,
    dc.clean_claim_rate,
    dc.denial_rate,
    dc.first_pass_resolution_rate,
    dc.net_collection_rate,
    dc.total_rework_hours,
    
    -- Denial metrics
    dd.denial_count,
    dd.denied_amount,
    dd.avg_days_to_resolution,
    dd.overturned_count,
    dd.avg_recovery_rate,
    dd.denial_rework_hours,
    dd.avg_recovery_per_hour,
    dd.aged_open_count,
    dd.at_risk_writeoff_count,
    
    -- Calculated denial metrics
    CASE 
        WHEN dd.denial_count > 0 
        THEN dd.overturned_count::NUMERIC / dd.denial_count 
        ELSE 0 
    END AS overturn_rate,
    
    -- Metadata
    CURRENT_TIMESTAMP AS _created_at

FROM daily_claims dc
LEFT JOIN daily_denials dd 
    ON dc.metric_date = dd.metric_date 
    AND dc.payer_type = dd.payer_type;

-- Indexes for fast dashboard queries
CREATE INDEX idx_metrics_daily_date ON analytics.metrics_rcm_daily(metric_date);
CREATE INDEX idx_metrics_daily_payer ON analytics.metrics_rcm_daily(payer_type);
CREATE INDEX idx_metrics_daily_specialty ON analytics.metrics_rcm_daily(specialty);
CREATE INDEX idx_metrics_daily_denial_type ON analytics.metrics_rcm_daily(denial_type_grouped);

-- Composite indexes for common filters
CREATE INDEX idx_metrics_daily_date_payer ON analytics.metrics_rcm_daily(metric_date, payer_type);
CREATE INDEX idx_metrics_daily_date_specialty ON analytics.metrics_rcm_daily(metric_date, specialty);

-- Permissions
GRANT SELECT ON analytics.metrics_rcm_daily TO tableau_user;

-- Statistics
ANALYZE analytics.metrics_rcm_daily;

-- Monitoring
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date)
SELECT 'metrics_rcm_daily', COUNT(*), CURRENT_DATE FROM analytics.metrics_rcm_daily;
