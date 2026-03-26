-- sql/03_marts/fct_bookings_attainment.sql
-- Description: Bookings attainment fact table for rep and manager performance

DROP TABLE IF EXISTS analytics.fct_bookings_attainment;

CREATE TABLE analytics.fct_bookings_attainment AS
SELECT
    owner_id,
    manager_id,
    segment,
    region,
    quota_month,
    quota_amount,
    booking_count,
    bookings_amount,
    bookings_arr,
    quota_variance_amount,
    attainment_rate,
    CASE WHEN attainment_rate >= 1 THEN 1 ELSE 0 END AS hit_quota_flag,
    CASE WHEN attainment_rate >= 1.2 THEN 1 ELSE 0 END AS stretch_attainment_flag,
    CURRENT_TIMESTAMP AS _mart_created_at
FROM intermediate.int_bookings_vs_quota;

CREATE INDEX idx_fct_bookings_attainment_month ON analytics.fct_bookings_attainment(quota_month);
CREATE INDEX idx_fct_bookings_attainment_owner ON analytics.fct_bookings_attainment(owner_id);
