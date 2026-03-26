-- sql/01_staging/stg_actuals.sql
-- Description: Monthly actual financials at department and cost center grain

DROP TABLE IF EXISTS staging.stg_actuals;

CREATE TABLE staging.stg_actuals AS
SELECT
    actual_id,
    fiscal_month::DATE AS fiscal_month,
    department,
    cost_center,
    account,
    actual_amount::NUMERIC(18,2) AS actual_amount,
    source_system,
    created_at::TIMESTAMP AS _loaded_at
FROM raw.finance_actuals;

CREATE INDEX idx_stg_actuals_month_dept ON staging.stg_actuals(fiscal_month, department);
