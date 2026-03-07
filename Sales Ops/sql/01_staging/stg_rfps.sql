-- sql/01_staging/stg_rfps.sql
-- Description: Standardizes RFP operations records and participant coverage
-- Depends on: raw_data.rfps_raw

DROP TABLE IF EXISTS staging.stg_rfps;

CREATE TABLE staging.stg_rfps AS
WITH source AS (
    SELECT *
    FROM raw_data.rfps_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '365 days'
),

cleaned AS (
    SELECT
        rfp_id,
        account_id,
        owner_id,
        intake_date,
        due_date,
        submitted_date,
        status,
        outcome,
        legal_participant_id,
        finance_participant_id,
        product_participant_id,

        -- Business flags
        CASE WHEN status IN ('Submitted', 'Closed') THEN 1 ELSE 0 END AS is_submitted,
        CASE WHEN outcome = 'Won' THEN 1 ELSE 0 END AS is_won,
        CASE WHEN due_date < CURRENT_DATE AND status NOT IN ('Submitted', 'Closed') THEN 1 ELSE 0 END AS is_overdue,

        -- KPI calculations
        EXTRACT(DAY FROM (due_date - intake_date))::INTEGER AS sla_days,
        EXTRACT(DAY FROM (COALESCE(submitted_date, CURRENT_DATE) - intake_date))::INTEGER AS turnaround_days,
        (CASE WHEN legal_participant_id IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN finance_participant_id IS NOT NULL THEN 1 ELSE 0 END
         + CASE WHEN product_participant_id IS NOT NULL THEN 1 ELSE 0 END) AS participant_count,

        -- Metadata
        load_date AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM source
    WHERE rfp_id IS NOT NULL
)

SELECT * FROM cleaned;

CREATE INDEX idx_stg_rfps_rfp_id ON staging.stg_rfps(rfp_id);
CREATE INDEX idx_stg_rfps_owner_id ON staging.stg_rfps(owner_id);
CREATE INDEX idx_stg_rfps_intake_date ON staging.stg_rfps(intake_date);
CREATE INDEX idx_stg_rfps_due_date ON staging.stg_rfps(due_date);
CREATE INDEX idx_stg_rfps_status ON staging.stg_rfps(status);
