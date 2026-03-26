# Payer Performance / Managed Care Analytics

This project extends the healthcare analytics portfolio with payer contracting and managed care performance insights that are both executive-readable and operationally actionable.

## Scope
- Expected-vs-actual reimbursement modeling by payer and service line.
- Underpayment detection and exposure tracking.
- Contract compliance analytics (denial rate, first-pass paid rate, payment timeliness).
- Monthly payer scorecards with alert flags for executive reviews.
- Contract renegotiation prioritization and recommended interventions.

## Structure
- `sql/01_staging`: contract terms, fee schedules, and claim line staging.
- `sql/02_intermediate`: expected reimbursement, underpayment, and compliance logic.
- `sql/03_marts`: payer facts, monthly scorecard, and renegotiation priority output.
- `tests`: SQL-based data quality and schema checks.
- `scripts`: deployment automation.

## Key marts
- `analytics.fct_underpayment_analytics`
- `analytics.fct_contract_compliance`
- `analytics.metrics_payer_performance_monthly`
- `analytics.analysis_contract_renegotiation_priority`
