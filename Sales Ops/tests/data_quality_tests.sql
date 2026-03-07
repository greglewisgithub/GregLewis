-- Sample Sales Ops data quality tests

-- 1) Ensure no duplicate opportunity IDs in pipeline fact
SELECT opportunity_id, COUNT(*) AS row_count
FROM analytics.fct_sales_pipeline_lifecycle
GROUP BY opportunity_id
HAVING COUNT(*) > 1;

-- 2) Ensure RFP SLA values are non-negative
SELECT rfp_id, sla_days
FROM analytics.fct_rfp_operations
WHERE sla_days < 0;
