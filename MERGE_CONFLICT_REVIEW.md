# Merge Conflict Review: Finance Ops + Payer Performance PR

## Why conflicts are happening

Because this branch introduces large, multi-file additions plus edits to a shared file (`README.md`), conflicts are likely when `main` has moved in parallel.

The conflict types are typically:

1. **`README.md` content conflict (same hunks edited in both branches).**
2. **Add/Add conflicts in new project paths** (`Finance Ops/*`, `Payer Performance/*`) if files with the same names were added independently on `main`.
3. **Potential config divergence conflict** in project-level pipeline/deploy/test files if teams made parallel edits after initial scaffolding.

---

## Conflict 1: `README.md` shared-content edits

### Why this conflict occurs
- This PR modifies branding/portfolio text while `main` also updated top-level messaging and sections.
- Git cannot auto-merge when the same line ranges are changed on both branches.

### Practical solution A (recommended)
- **Keep `main` README as source of truth**, and move extension-specific copy into companion docs (`PROJECT_EXTENSIONS.md`, project READMEs).
- During merge conflict resolution: choose `main` for overlapping README hunks, then add at most one small link line to extension docs.

### Practical solution B
- **Manual blended merge**: retain `main`’s structure/order and append only concise, non-overlapping project bullets.
- Avoid large rewrites; keep PR README delta <10 lines to minimize future conflicts.

---

## Conflict 2: Add/Add for `Finance Ops/*` files

### Why this conflict occurs
- If `main` already has a `Finance Ops` scaffold or similarly named files, this branch’s files collide path-for-path.
- Git treats this as conflicting file identity, not a trivial append.

### Practical solution A (recommended)
- **Rebase onto latest `main`**, then for each conflicting file:
  1. keep `main` version,
  2. re-apply only missing logic from this branch,
  3. preserve one canonical file per path.
- Prioritize SQL model semantics over comments/formatting.

### Practical solution B
- **Namespace this branch as v2 paths** temporarily (e.g., `Finance Ops/sql_v2/...`), merge cleanly, then do a follow-up consolidation PR into canonical paths.
- Useful when conflict volume is too high for one pass.

---

## Conflict 3: Add/Add for `Payer Performance/*` files

### Why this conflict occurs
- Same as Finance Ops: path-level collisions if parallel implementation landed in `main`.

### Practical solution A (recommended)
- **File-by-file three-way reconcile**:
  - choose one base file (usually `main`),
  - merge in missing columns/metrics/tests from this branch,
  - run SQL lint + test scripts after each directory layer (`01_staging`, `02_intermediate`, `03_marts`).

### Practical solution B
- **Feature-flag merge**:
  - merge only non-colliding subsets first (e.g., docs + staging),
  - then merge intermediate/marts in a second PR after aligning contracts/schemas with main.
- Reduces blast radius and review complexity.

---

## Conflict 4: Project pipeline/deploy/test config divergence

### Why this conflict occurs
- `bitbucket-pipelines.yml`, deploy scripts, or SQL test files may have diverged in env var names, DB names, or run order.

### Practical solution A (recommended)
- **Adopt `main` operational conventions first** (env vars, branch gates, database naming), then fold in new steps.
- Validate script syntax and run order immediately after resolution.

### Practical solution B
- **Extract shared conventions into a template** (or single docs standard), then normalize both projects to that template in a follow-up PR.
- Minimizes repeated config drift.

---

## Best-practice merge sequence (practical)
1. Update branch with latest `main`.
2. Resolve `README.md` first by favoring `main` structure.
3. Resolve `Finance Ops` files (staging → intermediate → marts → tests → scripts).
4. Resolve `Payer Performance` files in same order.
5. Resolve pipeline config last.
6. Run checks:
   - `bash -n "Finance Ops/scripts/deploy_financeops.sh"`
   - `bash -n "Payer Performance/scripts/deploy_payer_performance.sh"`
7. Open final merge PR with explicit "conflicts resolved" summary.
