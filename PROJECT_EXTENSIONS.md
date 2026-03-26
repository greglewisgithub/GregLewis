# Project Extensions: Finance Ops + Payer Performance

This document captures two SQL-first portfolio extensions added without modifying existing project directories, to minimize merge conflicts with `main`.

## Finance Ops
- Bookings vs forecast lifecycle.
- Quota and attainment.
- ARR / MRR movement.
- Revenue leakage exposure.
- Budget vs actuals.
- Cash conversion timing.
- Forecast accuracy by segment / rep / region.

Directory: `Finance Ops/`

## Payer Performance / Managed Care
- Contract terms and fee schedule normalization.
- Expected vs actual reimbursement at claim line grain.
- Underpayment surveillance and compliance KPIs.
- Executive payer scorecards with alert flags.
- Contract renegotiation prioritization outputs.

Directory: `Payer Performance/`

## Merge-safety approach
- New work is isolated to new top-level project directories plus this companion document.
- Existing domain code (`RCM/`, `Sales Ops/`) remains untouched.
