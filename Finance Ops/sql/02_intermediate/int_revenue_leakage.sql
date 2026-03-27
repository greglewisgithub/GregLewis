-- sql/02_intermediate/int_revenue_leakage.sql
-- Description: Revenue leakage bridge from booked value to realized cash and denials

DROP TABLE IF EXISTS intermediate.int_revenue_leakage;

CREATE TABLE intermediate.int_revenue_leakage AS
WITH booking_base AS (
    SELECT
        b.booking_id,
        b.account_id,
        b.owner_id,
        b.segment,
        b.region,
        b.booking_date,
        b.booking_amount,
        cc.cash_received,
        cc.days_to_cash,
        COALESCE(SUM(c.denied_amount), 0) AS linked_denied_amount
    FROM staging.stg_bookings b
    LEFT JOIN intermediate.int_cash_conversion cc
      ON b.booking_id = cc.booking_id
    LEFT JOIN analytics.fct_claims_lifecycle c
      ON b.account_id = c.patient_id
     AND DATE_TRUNC('month', c.claim_submitted_date)::DATE = DATE_TRUNC('month', b.booking_date)::DATE
    GROUP BY
        b.booking_id,
        b.account_id,
        b.owner_id,
        b.segment,
        b.region,
        b.booking_date,
        b.booking_amount,
        cc.cash_received,
        cc.days_to_cash
)
SELECT
    booking_id,
    account_id,
    owner_id,
    segment,
    region,
    booking_date,
    booking_amount,
    COALESCE(cash_received, 0) AS cash_received,
    COALESCE(days_to_cash, 0) AS days_to_cash,
    linked_denied_amount,
    booking_amount - COALESCE(cash_received, 0) AS gross_leakage_amount,
    GREATEST(booking_amount - COALESCE(cash_received, 0), 0) + linked_denied_amount AS total_leakage_exposure,
    (booking_amount - COALESCE(cash_received, 0)) / NULLIF(booking_amount, 0) AS gross_leakage_rate,
    CURRENT_TIMESTAMP AS _transformed_at
FROM booking_base;

CREATE INDEX idx_int_revenue_leakage_date ON intermediate.int_revenue_leakage(booking_date);
CREATE INDEX idx_int_revenue_leakage_owner ON intermediate.int_revenue_leakage(owner_id);
