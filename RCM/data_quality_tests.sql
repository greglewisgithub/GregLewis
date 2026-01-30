-- tests/data_quality_tests.sql
-- Description: Comprehensive data quality validation
-- Run: Every pipeline execution
-- JIRA: RCM-501

-- Test 1: Check for duplicate claim IDs in fact table
DO $$
DECLARE
    duplicate_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO duplicate_count
    FROM (
        SELECT claim_id, COUNT(*) 
        FROM analytics.fct_claims_lifecycle 
        GROUP BY claim_id 
        HAVING COUNT(*) > 1
    ) dups;
    
    IF duplicate_count > 0 THEN
        RAISE EXCEPTION 'Data Quality Failure: % duplicate claim_ids found in fct_claims_lifecycle', duplicate_count;
    ELSE
        RAISE NOTICE 'PASS: No duplicate claim_ids in fct_claims_lifecycle';
    END IF;
END $$;

-- Test 2: Validate net collection rate is between 0 and 1
DO $$
DECLARE
    invalid_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO invalid_count
    FROM analytics.fct_claims_lifecycle
    WHERE net_collection_rate < 0 OR net_collection_rate > 1.5;  -- Allow some overpayment
    
    IF invalid_count > 0 THEN
        RAISE EXCEPTION 'Data Quality Failure: % claims have invalid net_collection_rate', invalid_count;
    ELSE
        RAISE NOTICE 'PASS: All net_collection_rates are valid';
    END IF;
END $$;

-- Test 3: Check for NULL primary keys
DO $$
DECLARE
    null_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO null_count
    FROM analytics.fct_claims_lifecycle
    WHERE claim_id IS NULL;
    
    IF null_count > 0 THEN
        RAISE EXCEPTION 'Data Quality Failure: % NULL claim_ids in fct_claims_lifecycle', null_count;
    ELSE
        RAISE NOTICE 'PASS: No NULL claim_ids';
    END IF;
END $$;

-- Test 4: Validate denial amounts match between tables
DO $$
DECLARE
    mismatch_pct NUMERIC;
BEGIN
    WITH claims_denied AS (
        SELECT SUM(denied_amount) AS total FROM analytics.fct_claims_lifecycle
    ),
    denials_total AS (
        SELECT SUM(denied_amount) AS total FROM analytics.fct_denial_analytics
    )
    SELECT ABS(c.total - d.total) / NULLIF(c.total, 0) INTO mismatch_pct
    FROM claims_denied c, denials_total d;
    
    IF mismatch_pct > 0.01 THEN  -- Allow 1% variance
        RAISE EXCEPTION 'Data Quality Failure: Denial amounts mismatch by %% between tables', mismatch_pct * 100;
    ELSE
        RAISE NOTICE 'PASS: Denial amounts match between fact tables';
    END IF;
END $$;

-- Test 5: Check for future dates
DO $$
DECLARE
    future_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO future_count
    FROM analytics.fct_claims_lifecycle
    WHERE claim_submitted_date > CURRENT_DATE;
    
    IF future_count > 0 THEN
        RAISE EXCEPTION 'Data Quality Failure: % claims have future submission dates', future_count;
    ELSE
        RAISE NOTICE 'PASS: No future-dated claims';
    END IF;
END $$;

-- Test 6: Validate row counts vs. expected growth
DO $$
DECLARE
    current_count INTEGER;
    yesterday_count INTEGER;
    growth_rate NUMERIC;
BEGIN
    SELECT COUNT(*) INTO current_count FROM analytics.fct_claims_lifecycle;
    
    SELECT row_count INTO yesterday_count 
    FROM analytics.pipeline_metrics 
    WHERE table_name = 'fct_claims_lifecycle' 
        AND run_date = CURRENT_DATE - 1;
    
    IF yesterday_count IS NOT NULL THEN
        growth_rate := (current_count - yesterday_count)::NUMERIC / NULLIF(yesterday_count, 0);
        
        IF growth_rate > 0.50 OR growth_rate < -0.10 THEN
            RAISE WARNING 'Anomaly: Row count changed by %% from yesterday', growth_rate * 100;
        ELSE
            RAISE NOTICE 'PASS: Row count growth within expected range (%%)', growth_rate * 100;
        END IF;
    END IF;
END $$;

-- Test 7: Validate referential integrity
DO $$
DECLARE
    orphan_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO orphan_count
    FROM analytics.fct_denial_analytics d
    LEFT JOIN analytics.fct_claims_lifecycle c ON d.claim_id = c.claim_id
    WHERE c.claim_id IS NULL;
    
    IF orphan_count > 0 THEN
        RAISE WARNING 'Data Quality Warning: % denials have no matching claim', orphan_count;
    ELSE
        RAISE NOTICE 'PASS: All denials have matching claims';
    END IF;
END $$;

-- Log test completion
INSERT INTO analytics.pipeline_metrics (table_name, row_count, run_date, notes)
VALUES ('data_quality_tests', 7, CURRENT_DATE, 'All tests executed');
