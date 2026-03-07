-- sql/02_intermediate/int_rfp_lifecycle.sql
-- Description: Tracks RFP SLA adherence, handoffs, and collaboration coverage

DROP TABLE IF EXISTS intermediate.int_rfp_lifecycle;

CREATE TABLE intermediate.int_rfp_lifecycle AS
WITH rfps AS (
    SELECT * FROM staging.stg_rfps
),
activity AS (
    SELECT
        account_id,
        rep_id,
        MIN(activity_date) AS first_activity_date,
        MAX(activity_date) AS last_activity_date,
        COUNT(*) AS activity_count
    FROM staging.stg_activities
    GROUP BY 1, 2
),
enriched AS (
    SELECT
        r.rfp_id,
        r.account_id,
        r.owner_id,
        r.intake_date,
        r.due_date,
        r.submitted_date,
        r.status,
        r.outcome,
        r.participant_count,
        a.first_activity_date,
        a.last_activity_date,
        COALESCE(a.activity_count, 0) AS activity_count,

        -- Business flags
        r.is_submitted,
        r.is_won,
        r.is_overdue,
        CASE WHEN r.participant_count >= 2 THEN 1 ELSE 0 END AS has_cross_functional_coverage,

        -- KPI calculations
        r.sla_days,
        r.turnaround_days,
        EXTRACT(DAY FROM (COALESCE(a.first_activity_date, r.due_date) - r.intake_date))::INTEGER AS handoff_timeliness_days,
        CASE WHEN r.turnaround_days <= r.sla_days THEN 1 ELSE 0 END AS met_sla,

        -- Metadata
        r._loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM rfps r
    LEFT JOIN activity a ON r.account_id = a.account_id AND r.owner_id = a.rep_id
)
SELECT * FROM enriched;

CREATE INDEX idx_int_rfp_rfp_id ON intermediate.int_rfp_lifecycle(rfp_id);
CREATE INDEX idx_int_rfp_owner_id ON intermediate.int_rfp_lifecycle(owner_id);
CREATE INDEX idx_int_rfp_due_date ON intermediate.int_rfp_lifecycle(due_date);
CREATE INDEX idx_int_rfp_status ON intermediate.int_rfp_lifecycle(status);
