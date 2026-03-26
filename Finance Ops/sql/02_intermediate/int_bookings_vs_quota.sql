-- sql/02_intermediate/int_bookings_vs_quota.sql
-- Description: Reconciliation of bookings against monthly quota

DROP TABLE IF EXISTS intermediate.int_bookings_vs_quota;

CREATE TABLE intermediate.int_bookings_vs_quota AS
WITH bookings AS (
    SELECT
        owner_id,
        segment,
        region,
        DATE_TRUNC('month', booking_date)::DATE AS quota_month,
        COUNT(*) AS booking_count,
        SUM(booking_amount) AS bookings_amount,
        SUM(annualized_arr) AS bookings_arr
    FROM staging.stg_bookings
    GROUP BY 1, 2, 3, 4
)
SELECT
    q.owner_id,
    q.manager_id,
    q.segment,
    q.region,
    q.quota_month,
    q.quota_amount,
    COALESCE(b.booking_count, 0) AS booking_count,
    COALESCE(b.bookings_amount, 0) AS bookings_amount,
    COALESCE(b.bookings_arr, 0) AS bookings_arr,
    COALESCE(b.bookings_amount, 0) - q.quota_amount AS quota_variance_amount,
    COALESCE(b.bookings_amount, 0)::NUMERIC / NULLIF(q.quota_amount, 0) AS attainment_rate,
    CURRENT_TIMESTAMP AS _transformed_at
FROM staging.stg_quota q
LEFT JOIN bookings b
    ON q.owner_id = b.owner_id
   AND q.quota_month = b.quota_month;

CREATE INDEX idx_int_bookings_vs_quota_month ON intermediate.int_bookings_vs_quota(quota_month);
