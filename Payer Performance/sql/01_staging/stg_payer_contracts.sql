-- sql/01_staging/stg_payer_contracts.sql
-- Description: Standardized payer contract terms and reimbursement parameters

DROP TABLE IF EXISTS staging.stg_payer_contracts;

CREATE TABLE staging.stg_payer_contracts AS
SELECT
    contract_id,
    payer_id,
    payer_name,
    payer_type,
    service_line,
    specialty,
    cpt_code,
    place_of_service,
    effective_start_date::DATE AS effective_start_date,
    effective_end_date::DATE AS effective_end_date,
    contracted_rate::NUMERIC(18,2) AS contracted_rate,
    contracted_percent_of_charge::NUMERIC(9,4) AS contracted_percent_of_charge,
    timely_filing_days,
    filing_limit_appeal_days,
    source_system,
    created_at::TIMESTAMP AS _loaded_at
FROM raw.payer_contract_terms;

CREATE INDEX idx_stg_payer_contracts_payer ON staging.stg_payer_contracts(payer_id);
CREATE INDEX idx_stg_payer_contracts_dates ON staging.stg_payer_contracts(effective_start_date, effective_end_date);
