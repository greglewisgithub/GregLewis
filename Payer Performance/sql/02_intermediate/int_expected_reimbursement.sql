-- sql/02_intermediate/int_expected_reimbursement.sql
-- Description: Expected reimbursement and variance at claim line grain

DROP TABLE IF EXISTS intermediate.int_expected_reimbursement;

CREATE TABLE intermediate.int_expected_reimbursement AS
WITH contract_match AS (
    SELECT
        cl.*,
        pc.payer_name,
        pc.payer_type,
        pc.service_line,
        pc.specialty,
        pc.place_of_service,
        pc.contracted_rate,
        pc.contracted_percent_of_charge,
        COALESCE(fs.allowed_amount, pc.contracted_rate, cl.allowed_amount) AS expected_allowed_amount,
        ROW_NUMBER() OVER (
            PARTITION BY cl.claim_line_id
            ORDER BY pc.effective_start_date DESC
        ) AS contract_rank
    FROM staging.stg_claim_lines cl
    LEFT JOIN staging.stg_payer_contracts pc
      ON cl.payer_id = pc.payer_id
     AND cl.cpt_code = pc.cpt_code
     AND cl.service_date BETWEEN pc.effective_start_date AND pc.effective_end_date
    LEFT JOIN staging.stg_fee_schedule fs
      ON cl.payer_id = fs.payer_id
     AND cl.cpt_code = fs.cpt_code
     AND cl.service_date BETWEEN fs.effective_start_date AND fs.effective_end_date
)
SELECT
    claim_line_id,
    claim_id,
    payer_id,
    payer_name,
    payer_type,
    service_line,
    specialty,
    cpt_code,
    modifier,
    place_of_service,
    units,
    charge_amount,
    billed_amount,
    allowed_amount,
    paid_amount,
    expected_allowed_amount,
    (expected_allowed_amount - allowed_amount) AS allowed_variance_amount,
    (expected_allowed_amount - paid_amount) AS underpayment_amount,
    (expected_allowed_amount - paid_amount) / NULLIF(expected_allowed_amount, 0) AS underpayment_rate,
    denial_reason_code,
    service_date,
    claim_submitted_date,
    _loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM contract_match
WHERE contract_rank = 1;

CREATE INDEX idx_int_expected_reimbursement_claim ON intermediate.int_expected_reimbursement(claim_id);
CREATE INDEX idx_int_expected_reimbursement_payer ON intermediate.int_expected_reimbursement(payer_id);
