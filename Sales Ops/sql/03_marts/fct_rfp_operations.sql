-- sql/03_marts/fct_rfp_operations.sql
-- Description: RFP operations fact table with SLA and collaboration performance

DROP TABLE IF EXISTS analytics.fct_rfp_operations;

CREATE TABLE analytics.fct_rfp_operations AS
WITH rfp AS (
    SELECT * FROM intermediate.int_rfp_lifecycle
)
SELECT
    rfp_id,
    account_id,
    owner_id,
    intake_date,
    due_date,
    submitted_date,
    status,
    outcome,
    participant_count,
    activity_count,
    sla_days,
    turnaround_days,
    handoff_timeliness_days,

    -- Business flags
    is_submitted,
    is_won,
    is_overdue,
    has_cross_functional_coverage,
    met_sla,

    -- KPI calculations
    CASE WHEN is_submitted = 1 THEN turnaround_days::NUMERIC / NULLIF(sla_days, 0) ELSE NULL END AS sla_burn_ratio,
    CASE WHEN has_cross_functional_coverage = 1 AND met_sla = 1 THEN 1 ELSE 0 END AS ideal_execution_flag,

    -- Metadata
    _loaded_at,
    CURRENT_TIMESTAMP AS _transformed_at
FROM rfp;

CREATE INDEX idx_fct_rfp_rfp_id ON analytics.fct_rfp_operations(rfp_id);
CREATE INDEX idx_fct_rfp_owner_id ON analytics.fct_rfp_operations(owner_id);
CREATE INDEX idx_fct_rfp_intake_date ON analytics.fct_rfp_operations(intake_date);
CREATE INDEX idx_fct_rfp_status ON analytics.fct_rfp_operations(status);
