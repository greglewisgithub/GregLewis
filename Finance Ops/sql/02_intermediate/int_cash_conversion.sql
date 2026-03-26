-- sql/02_intermediate/int_cash_conversion.sql
-- Description: Booking to cash conversion bridge for liquidity and collections trend analysis

DROP TABLE IF EXISTS intermediate.int_cash_conversion;

CREATE TABLE intermediate.int_cash_conversion AS
WITH booking_cash AS (
    SELECT
        b.booking_id,
        b.owner_id,
        b.segment,
        b.region,
        b.booking_date,
        b.booking_amount,
        c.first_payment_date,
        c.actual_payments_received AS cash_received,
        c.days_to_payment
    FROM staging.stg_bookings b
    LEFT JOIN analytics.fct_claims_lifecycle c
      ON b.account_id = c.patient_id
)
SELECT
    booking_id,
    owner_id,
    segment,
    region,
    booking_date,
    booking_amount,
    first_payment_date,
    COALESCE(cash_received, 0) AS cash_received,
    COALESCE(days_to_payment, 0) AS days_to_cash,
    COALESCE(cash_received, 0) / NULLIF(booking_amount, 0) AS cash_realization_rate,
    CURRENT_TIMESTAMP AS _transformed_at
FROM booking_cash;

CREATE INDEX idx_int_cash_conversion_booking_date ON intermediate.int_cash_conversion(booking_date);
