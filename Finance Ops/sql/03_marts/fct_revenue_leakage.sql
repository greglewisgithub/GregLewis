-- sql/03_marts/fct_revenue_leakage.sql
-- Description: Revenue leakage fact table at monthly owner/segment/region grain

DROP TABLE IF EXISTS analytics.fct_revenue_leakage;

CREATE TABLE analytics.fct_revenue_leakage AS
SELECT
    DATE_TRUNC('month', booking_date)::DATE AS metric_month,
    owner_id,
    segment,
    region,
    COUNT(*) AS booking_count,
    SUM(booking_amount) AS booked_amount,
    SUM(cash_received) AS cash_received,
    SUM(gross_leakage_amount) AS gross_leakage_amount,
    SUM(linked_denied_amount) AS denied_amount,
    SUM(total_leakage_exposure) AS total_leakage_exposure,
    AVG(days_to_cash) AS avg_days_to_cash,
    SUM(gross_leakage_amount) / NULLIF(SUM(booking_amount), 0) AS gross_leakage_rate,
    SUM(total_leakage_exposure) / NULLIF(SUM(booking_amount), 0) AS total_leakage_exposure_rate,
    CURRENT_TIMESTAMP AS _mart_created_at
FROM intermediate.int_revenue_leakage
GROUP BY 1, 2, 3, 4;

CREATE INDEX idx_fct_revenue_leakage_month ON analytics.fct_revenue_leakage(metric_month);
CREATE INDEX idx_fct_revenue_leakage_owner ON analytics.fct_revenue_leakage(owner_id);
