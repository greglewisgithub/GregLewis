#!/usr/bin/env bash
set -euo pipefail

# Sample deployment script for Sales Ops SQL layers.
# Usage:
#   ./scripts/deploy_salesops.sh \
#     --db-host "$DEV_DB_HOST" \
#     --db-user "$DEV_DB_USER" \
#     --db-name "salesops_analytics" \
#     --db-password "$DEV_DB_PASSWORD"

DB_HOST=""
DB_USER=""
DB_NAME="salesops_analytics"
DB_PASSWORD=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --db-host)
      DB_HOST="$2"; shift 2 ;;
    --db-user)
      DB_USER="$2"; shift 2 ;;
    --db-name)
      DB_NAME="$2"; shift 2 ;;
    --db-password)
      DB_PASSWORD="$2"; shift 2 ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 1 ;;
  esac
done

if [[ -z "$DB_HOST" || -z "$DB_USER" || -z "$DB_PASSWORD" ]]; then
  echo "Missing required DB connection arguments." >&2
  exit 1
fi

export PGPASSWORD="$DB_PASSWORD"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_FILE="$ROOT_DIR/scripts/deploy_salesops_$(date +%Y%m%d_%H%M%S).log"

run_sql_file() {
  local sql_file="$1"
  echo "Executing $sql_file" | tee -a "$LOG_FILE"

  psql \
    -h "$DB_HOST" \
    -U "$DB_USER" \
    -d "$DB_NAME" \
    -v ON_ERROR_STOP=1 \
    -f "$sql_file" | tee -a "$LOG_FILE"
}

echo "Starting Sales Ops deployment at $(date -u +"%Y-%m-%dT%H:%M:%SZ")" | tee -a "$LOG_FILE"

echo "=== Layer 1: staging ===" | tee -a "$LOG_FILE"
for file in "$ROOT_DIR"/sql/01_staging/*.sql; do
  run_sql_file "$file"
done

echo "=== Layer 2: intermediate ===" | tee -a "$LOG_FILE"
for file in "$ROOT_DIR"/sql/02_intermediate/*.sql; do
  run_sql_file "$file"
done

echo "=== Layer 3: marts ===" | tee -a "$LOG_FILE"
for file in "$ROOT_DIR"/sql/03_marts/*.sql; do
  run_sql_file "$file"
done

# Optional sample checks (safe to keep even if files are empty placeholders)
if [[ -f "$ROOT_DIR/tests/data_quality_tests.sql" ]]; then
  echo "=== Running data quality tests ===" | tee -a "$LOG_FILE"
  run_sql_file "$ROOT_DIR/tests/data_quality_tests.sql"
fi

if [[ -f "$ROOT_DIR/tests/schema_tests.sql" ]]; then
  echo "=== Running schema tests ===" | tee -a "$LOG_FILE"
  run_sql_file "$ROOT_DIR/tests/schema_tests.sql"
fi

echo "Sales Ops deployment completed successfully at $(date -u +"%Y-%m-%dT%H:%M:%SZ")" | tee -a "$LOG_FILE"
