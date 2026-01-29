-- sql/04_analysis/denial_root_cause_priority.sql
-- Description: Root cause analysis with intervention priority scoring
-- Used by: Denial Deep Dive Dashboard
-- JIRA: RCM-401

-- This query powers the "High-Priority Denials for Rework" table in Tableau
-- Run date range and filters are parameterized in Tableau

WITH denial_drivers AS (
    SELECT
        denial_type_grouped,
        denial_reason_code,
        denial_reason_description,
        preventability_category,
        payer_name,
        specialty,
        financial_impact_tier,
        
        -- Aggregated metrics
        COUNT(*) AS denial_count,
        SUM(denied_amount) AS total_denied_amount,
        AVG(days_to_resolution) AS avg_resolution_days,
        SUM(rework_hours) AS total_rework_hours,
        SUM(is_overturned) AS overturn_count,
        AVG(recovery_rate) AS avg_recovery_rate,
        SUM(is_aged_open_denial) AS aged_open_count,
        
        -- Percentage of total denials
        SUM(denied_amount) / SUM(SUM(denied_amount)) OVER () AS pct_of_total_denied
        
    FROM analytics.fct_denial_analytics
    WHERE denial_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

ranked_drivers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY denial_type_grouped 
            ORDER BY total_denied_amount DESC
        ) AS rank_within_type,
        
        -- Multi-factor priority score
        -- Weight: Financial impact (40%), Rework burden (30%), Volume (20%), Preventability (10%)
        (
            (total_denied_amount * 0.40) +
            (total_rework_hours * 100 * 0.30) +
            (denial_count * 50 * 0.20) +
            (CASE 
                WHEN preventability_category = 'Highly Preventable' THEN 1000
                WHEN preventability_category = 'Preventable' THEN 500
                ELSE 100
            END * 0.10)
        ) AS intervention_priority_score
        
    FROM denial_drivers
),

actionable_denials AS (
    SELECT
        denial_type_grouped,
        denial_reason_code,
        denial_reason_description,
        preventability_category,
        payer_name,
        specialty,
        financial_impact_tier,
        denial_count,
        total_denied_amount,
        pct_of_total_denied,
        avg_resolution_days,
        total_rework_hours,
        overturn_count,
        avg_recovery_rate,
        aged_open_count,
        intervention_priority_score,
        rank_within_type,
        
        -- ROI estimate for intervention
        CASE 
            WHEN preventability_category IN ('Highly Preventable', 'Preventable')
            THEN total_denied_amount * 0.75  -- Assume 75% reduction possible
            ELSE total_denied_amount * 0.25  -- Lower potential for "Other"
        END AS estimated_recovery_potential,
        
        -- Recommended action
        CASE 
            WHEN preventability_category = 'Highly Preventable' 
                AND denial_type_grouped = 'Authorization'
            THEN 'Implement pre-submission authorization checklist'
            
            WHEN preventability_category = 'Highly Preventable' 
                AND denial_type_grouped = 'Eligibility'
            THEN 'Enable real-time eligibility verification at scheduling'
            
            WHEN preventability_category = 'Preventable' 
                AND denial_type_grouped = 'Documentation'
            THEN 'Deploy structured documentation templates'
            
            WHEN preventability_category = 'Preventable' 
                AND denial_type_grouped = 'Coding'
            THEN 'Provider coding education and scrubber rules'
            
            WHEN denial_type_grouped = 'Timely Filing'
            THEN 'Accelerate denial rework workflow'
            
            ELSE 'Deep dive analysis required'
        END AS recommended_intervention
        
    FROM ranked_drivers
    WHERE rank_within_type <= 5  -- Top 5 within each denial type
)

SELECT 
    *,
    -- Quarterly savings if resolved
    (estimated_recovery_potential * 4) AS estimated_annual_impact
FROM actionable_denials
ORDER BY intervention_priority_score DESC
LIMIT 50;

-- This query result feeds the Tableau dashboard's priority table
-- Tableau applies additional user-selected filters (date range, payer, etc.)
