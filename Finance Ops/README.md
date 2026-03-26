# Finance Ops Analytics

This project extends the repository's operational analytics framework into finance performance management.

## Scope
- Bookings versus forecast performance.
- Forecast accuracy by segment, rep, and region.
- Quota and attainment monitoring.
- Revenue forecast lifecycle tracking.
- Bookings-to-quota attainment analysis.
- ARR / MRR movement monitoring.
- Revenue leakage exposure tracking.
- Budget versus actual variance monitoring.
- Cash conversion timing analysis.
- Monthly and quarterly executive KPI marts.

## Structure
- `sql/01_staging`: Source-normalized inputs for bookings, forecast, quota, budget, and actuals.
- `sql/02_intermediate`: Enriched business logic for forecast blending, attainment, and variance models.
- `sql/03_marts`: Fact tables and KPI aggregates for executive reporting.
- `tests`: Data quality and schema validation SQL.
- `scripts`: Deployment automation.

## Key marts
- `analytics.fct_revenue_forecast_lifecycle`
- `analytics.fct_bookings_attainment`
- `analytics.fct_budget_actual_variance`
- `analytics.fct_arr_mrr_movement`
- `analytics.fct_revenue_leakage`
- `analytics.metrics_financeops_monthly`
- `analytics.metrics_financeops_quarterly`
