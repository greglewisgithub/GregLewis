-- sql/03_marts/fct_budget_actual_variance.sql
-- Description: Monthly finance performance table for budget governance

DROP TABLE IF EXISTS analytics.fct_budget_actual_variance;

CREATE TABLE analytics.fct_budget_actual_variance AS
SELECT
    fiscal_month,
    department,
    cost_center,
    budget_category,
    planned_amount,
    actual_amount,
    variance_amount,
    variance_pct,
    CASE
      WHEN variance_pct IS NULL THEN 'No Budget'
      WHEN variance_pct <= -0.05 THEN 'Favorable'
      WHEN variance_pct >= 0.05 THEN 'Unfavorable'
      ELSE 'On Plan'
    END AS variance_status,
    CURRENT_TIMESTAMP AS _mart_created_at
FROM intermediate.int_budget_vs_actuals;

CREATE INDEX idx_fct_budget_variance_month ON analytics.fct_budget_actual_variance(fiscal_month);
CREATE INDEX idx_fct_budget_variance_department ON analytics.fct_budget_actual_variance(department);
