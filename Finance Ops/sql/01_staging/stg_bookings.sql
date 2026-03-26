-- sql/01_staging/stg_bookings.sql
-- Description: Standardized bookings source for Finance Ops modeling

DROP TABLE IF EXISTS staging.stg_bookings;

CREATE TABLE staging.stg_bookings AS
SELECT
    booking_id,
    opportunity_id,
    account_id,
    owner_id,
    segment,
    region,
    product_family,
    booking_date::DATE AS booking_date,
    close_date::DATE AS close_date,
    contract_start_date::DATE AS contract_start_date,
    contract_end_date::DATE AS contract_end_date,
    booking_amount::NUMERIC(18,2) AS booking_amount,
    annualized_arr::NUMERIC(18,2) AS annualized_arr,
    billing_frequency,
    payment_terms_days,
    source_system,
    created_at::TIMESTAMP AS _loaded_at
FROM raw.bookings;

CREATE INDEX idx_stg_bookings_booking_date ON staging.stg_bookings(booking_date);
CREATE INDEX idx_stg_bookings_owner_id ON staging.stg_bookings(owner_id);
