-- Sample Sales Ops schema tests

-- 1) Confirm core marts exist
SELECT table_schema, table_name
FROM information_schema.tables
WHERE (table_schema, table_name) IN (
  ('analytics', 'fct_sales_pipeline_lifecycle'),
  ('analytics', 'fct_rfp_operations'),
  ('analytics', 'metrics_salesops_daily'),
  ('analytics', 'metrics_salesops_weekly'),
  ('analytics', 'metrics_salesops_monthly'),
  ('analytics', 'metrics_salesops_quarterly')
);

-- 2) Confirm metadata columns exist on daily metrics table
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'metrics_salesops_daily'
  AND column_name IN ('_loaded_at', '_transformed_at');
