-- sql/03_marts/fct_arr_mrr_movement.sql
-- Description: ARR/MRR movement fact table for retention and expansion performance

DROP TABLE IF EXISTS analytics.fct_arr_mrr_movement;

CREATE TABLE analytics.fct_arr_mrr_movement AS
SELECT
    DATE_TRUNC('month', booking_date)::DATE AS metric_month,
    owner_id,
    segment,
    region,
    movement_type,
    COUNT(*) AS movement_event_count,
    SUM(annualized_arr) AS ending_arr,
    SUM(prior_arr) AS starting_arr,
    SUM(arr_change) AS net_arr_change,
    SUM(mrr) AS ending_mrr,
    SUM(mrr_change) AS net_mrr_change,
    CURRENT_TIMESTAMP AS _mart_created_at
FROM intermediate.int_arr_mrr_movement
GROUP BY 1, 2, 3, 4, 5;

CREATE INDEX idx_fct_arr_mrr_movement_month ON analytics.fct_arr_mrr_movement(metric_month);
CREATE INDEX idx_fct_arr_mrr_movement_owner ON analytics.fct_arr_mrr_movement(owner_id);
