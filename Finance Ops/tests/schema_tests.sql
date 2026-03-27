-- Finance Ops schema tests

-- 1) Confirm core marts exist
SELECT table_schema, table_name
FROM information_schema.tables
WHERE (table_schema, table_name) IN (
  ('analytics', 'fct_revenue_forecast_lifecycle'),
  ('analytics', 'fct_bookings_attainment'),
  ('analytics', 'fct_budget_actual_variance'),
  ('analytics', 'fct_arr_mrr_movement'),
  ('analytics', 'fct_revenue_leakage'),
  ('analytics', 'metrics_financeops_monthly'),
  ('analytics', 'metrics_financeops_quarterly')
);

-- 2) Confirm metadata columns exist on monthly metrics table
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'metrics_financeops_monthly'
  AND column_name IN ('metric_month', 'owner_id', '_transformed_at');
