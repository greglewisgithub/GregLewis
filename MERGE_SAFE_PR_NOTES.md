# Merge-safe PR Notes

This branch is prepared to merge cleanly into `main` by keeping extension work isolated to new top-level directories:

- `Finance Ops/`
- `Payer Performance/`
- companion documentation files only.

## Conflict-minimizing approach
- No edits to `RCM/` and `Sales Ops/` trees.
- No schema object name overlap with existing marts in those domains.
- Project-specific CI/deploy files live inside each new directory.

## Suggested merge order
1. Merge this PR directly into `main`.
2. Run each project's SQL lint/test pipeline.
3. Execute deploy scripts in development before production promotion.
