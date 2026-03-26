-- sql/02_intermediate/int_arr_mrr_movement.sql
-- Description: ARR/MRR movement classification at booking event grain

DROP TABLE IF EXISTS intermediate.int_arr_mrr_movement;

CREATE TABLE intermediate.int_arr_mrr_movement AS
WITH bookings AS (
    SELECT
        booking_id,
        account_id,
        owner_id,
        segment,
        region,
        product_family,
        booking_date,
        annualized_arr,
        (annualized_arr / 12.0)::NUMERIC(18,2) AS mrr,
        LAG(annualized_arr) OVER (
            PARTITION BY account_id, product_family
            ORDER BY booking_date, booking_id
        ) AS prior_arr
    FROM staging.stg_bookings
)
SELECT
    booking_id,
    account_id,
    owner_id,
    segment,
    region,
    product_family,
    booking_date,
    annualized_arr,
    mrr,
    COALESCE(prior_arr, 0) AS prior_arr,
    annualized_arr - COALESCE(prior_arr, 0) AS arr_change,
    (annualized_arr - COALESCE(prior_arr, 0)) / 12.0 AS mrr_change,
    CASE
        WHEN COALESCE(prior_arr, 0) = 0 AND annualized_arr > 0 THEN 'New'
        WHEN annualized_arr > COALESCE(prior_arr, 0) THEN 'Expansion'
        WHEN annualized_arr < COALESCE(prior_arr, 0) AND annualized_arr > 0 THEN 'Contraction'
        WHEN annualized_arr = 0 AND COALESCE(prior_arr, 0) > 0 THEN 'Churn'
        ELSE 'Flat'
    END AS movement_type,
    CURRENT_TIMESTAMP AS _transformed_at
FROM bookings;

CREATE INDEX idx_int_arr_mrr_movement_date ON intermediate.int_arr_mrr_movement(booking_date);
CREATE INDEX idx_int_arr_mrr_movement_owner ON intermediate.int_arr_mrr_movement(owner_id);
