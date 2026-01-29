-- sql/01_staging/stg_denials.sql
-- Description: Standardizes denial data with categorization logic
-- Depends on: raw_data.denial_data
-- JIRA: RCM-102

DROP TABLE IF EXISTS staging.stg_denials;

CREATE TABLE staging.stg_denials AS
WITH source AS (
    SELECT * FROM raw_data.denial_data
    WHERE load_date >= CURRENT_DATE - INTERVAL '90 days'
),

cleaned AS (
    SELECT
        denial_id,
        claim_id,
        denial_date,
        denial_reason_code,
        denial_reason_description,
        denial_category,
        payer_id,
        denied_amount,
        denial_status, -- 'Open', 'Appealed', 'Overturned', 'Written Off'
        appeal_date,
        resolution_date,
        resolution_amount,
        rework_hours,
        
        -- Denial type classification based on CARC codes
        CASE 
            WHEN denial_reason_code IN ('CO-16', 'CO-18', 'CO-50', 'CO-151') 
                THEN 'Authorization'
            WHEN denial_reason_code IN ('CO-11', 'CO-197', 'CO-204')
                THEN 'Documentation'
            WHEN denial_reason_code IN ('CO-22', 'CO-26', 'CO-27')
                THEN 'Eligibility'
            WHEN denial_reason_code IN ('CO-4', 'CO-97', 'CO-109')
                THEN 'Coding'
            WHEN denial_reason_code IN ('CO-29', 'CO-96')
                THEN 'Timely Filing'
            ELSE 'Other'
        END AS denial_type_grouped,
        
        -- Binary flags for analysis
        CASE 
            WHEN denial_status = 'Overturned' THEN 1 
            ELSE 0 
        END AS is_overturned,
        
        CASE 
            WHEN denial_status = 'Open' THEN 1 
            ELSE 0 
        END AS is_open,
        
        CASE 
            WHEN denial_status = 'Written Off' THEN 1 
            ELSE 0 
        END AS is_written_off,
        
        -- Metadata
        load_date AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
        
    FROM source
    WHERE denial_id IS NOT NULL
)

--SELECT * FROM cleaned;

-- Indexes
CREATE INDEX idx_stg_denials_denial_id ON staging.stg_denials(denial_id);
CREATE INDEX idx_stg_denials_claim_id ON staging.stg_denials(claim_id);
CREATE INDEX idx_stg_denials_date ON staging.stg_denials(denial_date);
CREATE INDEX idx_stg_denials_status ON staging.stg_denials(denial_status);

-- Monitoring
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date)
SELECT 'stg_denials', COUNT(*), CURRENT_DATE FROM staging.stg_denials;
