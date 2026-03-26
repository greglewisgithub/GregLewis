-- Payer Performance schema tests

SELECT table_schema, table_name
FROM information_schema.tables
WHERE (table_schema, table_name) IN (
  ('analytics', 'fct_underpayment_analytics'),
  ('analytics', 'fct_contract_compliance'),
  ('analytics', 'metrics_payer_performance_monthly'),
  ('analytics', 'analysis_contract_renegotiation_priority')
);

SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'analytics'
  AND table_name = 'metrics_payer_performance_monthly'
  AND column_name IN ('metric_month', 'payer_id', 'underpayment_alert_flag', 'denial_alert_flag');
