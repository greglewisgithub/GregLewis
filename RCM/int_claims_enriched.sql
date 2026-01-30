-- sql/02_intermediate/int_claims_enriched.sql
-- Description: Enriches claims with denial and payment data
-- Depends on: staging.stg_claims, staging.stg_denials, staging.stg_payments
-- JIRA: RCM-201

DROP TABLE IF EXISTS intermediate.int_claims_enriched;

CREATE TABLE intermediate.int_claims_enriched AS
WITH claims AS (
    SELECT * FROM staging.stg_claims
),

-- Aggregate denial metrics per claim
claim_denials AS (
    SELECT
        claim_id,
        COUNT(*) AS denial_count,
        SUM(denied_amount) AS total_denied_amount,
        SUM(is_overturned) AS overturn_count,
        SUM(rework_hours) AS total_rework_hours,
        MAX(denial_date) AS latest_denial_date,
        STRING_AGG(DISTINCT denial_type_grouped, ', ' ORDER BY denial_type_grouped) AS denial_types,
        MAX(denial_status) AS latest_denial_status
    FROM staging.stg_denials
    GROUP BY claim_id
),

-- Aggregate payment metrics per claim
claim_payments AS (
    SELECT
        claim_id,
        SUM(transaction_amount) AS total_payments,
        MIN(transaction_date) AS first_payment_date,
        MAX(transaction_date) AS last_payment_date,
        COUNT(*) AS payment_count
    FROM staging.stg_payments
    WHERE transaction_type = 'PAYMENT'
    GROUP BY claim_id
),

-- Join payer dimension
payers AS (
    SELECT 
        payer_id,
        payer_name,
        payer_type
    FROM raw_data.payers
),

-- Join provider dimension
providers AS (
    SELECT 
        provider_id,
        provider_name,
        specialty
    FROM raw_data.providers
),

enriched AS (
    SELECT
        -- IDs
        c.claim_id,
        c.patient_id,
        c.payer_id,
        c.rendering_provider_id,
        
        -- Dates
        c.claim_created_date,
        c.claim_submitted_date,
        c.service_date,
        cp.first_payment_date,
        cp.last_payment_date,
        
        -- Amounts
        c.total_charge_amount,
        c.total_allowed_amount,
        c.total_paid_amount,
        COALESCE(cp.total_payments, 0) AS actual_payments_received,
        COALESCE(cd.total_denied_amount, 0) AS denied_amount,
        
        -- Claim attributes
        c.claim_status,
        c.claim_type,
        c.place_of_service,
        
        -- Denial metrics
        COALESCE(cd.denial_count, 0) AS denial_count,
        COALESCE(cd.overturn_count, 0) AS overturn_count,
        COALESCE(cd.total_rework_hours, 0) AS rework_hours,
        cd.denial_types,
        cd.latest_denial_status,
        
        -- Payment metrics
        COALESCE(cp.payment_count, 0) AS payment_count,
        
        -- Dimensions
        p.payer_name,
        p.payer_type,
        prov.provider_name,
        prov.specialty,
        
        -- Calculated flags
        CASE 
            WHEN cd.claim_id IS NOT NULL THEN 1 
            ELSE 0 
        END AS is_denied,
        
        CASE 
            WHEN cd.claim_id IS NULL THEN 1 
            ELSE 0 
        END AS is_clean_claim,
        
        CASE 
            WHEN c.claim_status = 'Paid' 
                AND cd.claim_id IS NULL THEN 1
            ELSE 0
        END AS is_first_pass_paid,
        
        -- Days calculations
        EXTRACT(DAY FROM (COALESCE(cp.first_payment_date, CURRENT_DATE) - c.claim_submitted_date))::INTEGER 
            AS days_to_payment,
        
        EXTRACT(DAY FROM (c.claim_submitted_date - c.service_date))::INTEGER 
            AS days_to_submission,
        
        -- Current AR age
        EXTRACT(DAY FROM (CURRENT_DATE - c.claim_submitted_date))::INTEGER 
            AS current_ar_age,
        
        -- AR aging bucket
        CASE
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - c.claim_submitted_date)) <= 30 THEN '0-30'
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - c.claim_submitted_date)) <= 60 THEN '31-60'
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - c.claim_submitted_date)) <= 90 THEN '61-90'
            WHEN EXTRACT(DAY FROM (CURRENT_DATE - c.claim_submitted_date)) <= 120 THEN '91-120'
            ELSE '120+'
        END AS aging_bucket,
        
        -- Metadata
        c._loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
        
    FROM claims c
    LEFT JOIN claim_denials cd ON c.claim_id = cd.claim_id
    LEFT JOIN claim_payments cp ON c.claim_id = cp.claim_id
    LEFT JOIN payers p ON c.payer_id = p.payer_id
    LEFT JOIN providers prov ON c.rendering_provider_id = prov.provider_id
)

--SELECT * FROM enriched;

-- Indexes for performance
CREATE INDEX idx_int_claims_claim_id ON intermediate.int_claims_enriched(claim_id);
CREATE INDEX idx_int_claims_submit_date ON intermediate.int_claims_enriched(claim_submitted_date);
CREATE INDEX idx_int_claims_payer ON intermediate.int_claims_enriched(payer_id);
CREATE INDEX idx_int_claims_status ON intermediate.int_claims_enriched(claim_status);
CREATE INDEX idx_int_claims_aging ON intermediate.int_claims_enriched(aging_bucket);

-- Monitoring
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date)
SELECT 'int_claims_enriched', COUNT(*), CURRENT_DATE FROM intermediate.int_claims_enriched;
