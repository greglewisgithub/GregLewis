-- sql/01_staging/stg_fee_schedule.sql
-- Description: Fee schedule rates by payer and procedure

DROP TABLE IF EXISTS staging.stg_fee_schedule;

CREATE TABLE staging.stg_fee_schedule AS
SELECT
    fee_schedule_id,
    payer_id,
    cpt_code,
    modifier,
    allowed_amount::NUMERIC(18,2) AS allowed_amount,
    effective_start_date::DATE AS effective_start_date,
    effective_end_date::DATE AS effective_end_date,
    source_system,
    created_at::TIMESTAMP AS _loaded_at
FROM raw.payer_fee_schedule;

CREATE INDEX idx_stg_fee_schedule_lookup ON staging.stg_fee_schedule(payer_id, cpt_code);
