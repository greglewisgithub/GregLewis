-- sql/02_intermediate/int_budget_vs_actuals.sql
-- Description: Budget versus actual variance at monthly department/category grain

DROP TABLE IF EXISTS intermediate.int_budget_vs_actuals;

CREATE TABLE intermediate.int_budget_vs_actuals AS
WITH actuals_agg AS (
    SELECT
        fiscal_month,
        department,
        cost_center,
        SUM(actual_amount) AS total_actual_amount
    FROM staging.stg_actuals
    GROUP BY 1, 2, 3
)
SELECT
    b.fiscal_month,
    b.department,
    b.cost_center,
    b.budget_category,
    b.planned_amount,
    COALESCE(a.total_actual_amount, 0) AS actual_amount,
    COALESCE(a.total_actual_amount, 0) - b.planned_amount AS variance_amount,
    CASE
        WHEN b.planned_amount = 0 THEN NULL
        ELSE (COALESCE(a.total_actual_amount, 0) - b.planned_amount) / b.planned_amount
    END AS variance_pct,
    CURRENT_TIMESTAMP AS _transformed_at
FROM staging.stg_budget b
LEFT JOIN actuals_agg a
    ON b.fiscal_month = a.fiscal_month
   AND b.department = a.department
   AND b.cost_center = a.cost_center;

CREATE INDEX idx_int_budget_vs_actuals_month ON intermediate.int_budget_vs_actuals(fiscal_month);
