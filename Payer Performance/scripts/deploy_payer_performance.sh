#!/usr/bin/env bash
set -euo pipefail

# Deployment script for Payer Performance SQL layers.

DB_HOST=""
DB_USER=""
DB_NAME="payer_performance_analytics"
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
LOG_FILE="$ROOT_DIR/scripts/deploy_payer_performance_$(date +%Y%m%d_%H%M%S).log"

run_sql_file() {
  local sql_file="$1"
  echo "Executing $sql_file" | tee -a "$LOG_FILE"
  psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 -f "$sql_file" | tee -a "$LOG_FILE"
}

echo "Starting Payer Performance deployment at $(date -u +"%Y-%m-%dT%H:%M:%SZ")" | tee -a "$LOG_FILE"
for layer in 01_staging 02_intermediate 03_marts; do
  echo "=== Layer: ${layer} ===" | tee -a "$LOG_FILE"
  for file in "$ROOT_DIR"/sql/${layer}/*.sql; do
    run_sql_file "$file"
  done
done

if [[ -f "$ROOT_DIR/tests/data_quality_tests.sql" ]]; then
  run_sql_file "$ROOT_DIR/tests/data_quality_tests.sql"
fi

if [[ -f "$ROOT_DIR/tests/schema_tests.sql" ]]; then
  run_sql_file "$ROOT_DIR/tests/schema_tests.sql"
fi

echo "Payer Performance deployment completed at $(date -u +"%Y-%m-%dT%H:%M:%SZ")" | tee -a "$LOG_FILE"
