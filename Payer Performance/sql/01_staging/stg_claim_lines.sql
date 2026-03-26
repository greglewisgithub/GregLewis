-- sql/01_staging/stg_claim_lines.sql
-- Description: Claim line detail for expected-vs-actual reimbursement analysis

DROP TABLE IF EXISTS staging.stg_claim_lines;

CREATE TABLE staging.stg_claim_lines AS
SELECT
    claim_line_id,
    claim_id,
    payer_id,
    cpt_code,
    modifier,
    units,
    charge_amount::NUMERIC(18,2) AS charge_amount,
    billed_amount::NUMERIC(18,2) AS billed_amount,
    allowed_amount::NUMERIC(18,2) AS allowed_amount,
    paid_amount::NUMERIC(18,2) AS paid_amount,
    denial_reason_code,
    service_date::DATE AS service_date,
    claim_submitted_date::DATE AS claim_submitted_date,
    source_system,
    created_at::TIMESTAMP AS _loaded_at
FROM raw.claim_lines;

CREATE INDEX idx_stg_claim_lines_claim ON staging.stg_claim_lines(claim_id);
CREATE INDEX idx_stg_claim_lines_payer_cpt ON staging.stg_claim_lines(payer_id, cpt_code);
