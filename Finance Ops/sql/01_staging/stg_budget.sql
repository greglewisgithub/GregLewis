-- sql/01_staging/stg_budget.sql
-- Description: Monthly operating budget at department and cost center grain

DROP TABLE IF EXISTS staging.stg_budget;

CREATE TABLE staging.stg_budget AS
SELECT
    budget_id,
    fiscal_month::DATE AS fiscal_month,
    department,
    cost_center,
    budget_category,
    planned_amount::NUMERIC(18,2) AS planned_amount,
    source_system,
    created_at::TIMESTAMP AS _loaded_at
FROM raw.finance_budget;

CREATE INDEX idx_stg_budget_month_dept ON staging.stg_budget(fiscal_month, department);
