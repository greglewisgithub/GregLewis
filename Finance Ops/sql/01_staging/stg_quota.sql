-- sql/01_staging/stg_quota.sql
-- Description: Sales quota assignments by period

DROP TABLE IF EXISTS staging.stg_quota;

CREATE TABLE staging.stg_quota AS
SELECT
    quota_id,
    owner_id,
    manager_id,
    segment,
    region,
    quota_month::DATE AS quota_month,
    quota_amount::NUMERIC(18,2) AS quota_amount,
    source_system,
    created_at::TIMESTAMP AS _loaded_at
FROM raw.quota_assignments;

CREATE INDEX idx_stg_quota_owner_month ON staging.stg_quota(owner_id, quota_month);
