#!/bin/bash
set -e

echo "Starting RCM Analytics Deployment..."

# Database connection parameters from environment variables
DB_HOST=${PROD_DB_HOST}
DB_USER=${PROD_DB_USER}
DB_NAME="rcm_analytics"
export PGPASSWORD=${PROD_DB_PASSWORD}

# Function to execute SQL file with error handling
execute_sql() {
    local file=$1
    echo "Executing: $file"
    
    if psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f "$file" -v ON_ERROR_STOP=1; then
        echo "✓ Successfully executed: $file"
    else
        echo "✗ Failed to execute: $file"
        exit 1
    fi
}

# Create deployment log
DEPLOYMENT_LOG="deployment_$(date +%Y%m%d_%H%M%S).log"
echo "Deployment started at $(date)" > $DEPLOYMENT_LOG

# Stage 1: Staging Tables
echo "=== Stage 1: Deploying Staging Layer ==="
for file in sql/01_staging/*.sql; do
    execute_sql "$file" | tee -a $DEPLOYMENT_LOG
done

# Stage 2: Intermediate Tables
echo "=== Stage 2: Deploying Intermediate Layer ==="
for file in sql/02_intermediate/*.sql; do
    execute_sql "$file" | tee -a $DEPLOYMENT_LOG
done

# Stage 3: Analytics Marts
echo "=== Stage 3: Deploying Analytics Marts ==="
for file in sql/03_marts/*.sql; do
    execute_sql "$file" | tee -a $DEPLOYMENT_LOG
done

# Validate deployment
echo "=== Running Post-Deployment Validation ==="
psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f tests/data_quality_tests.sql | tee -a $DEPLOYMENT_LOG

# Update deployment metadata
psql -h $DB_HOST -U $DB_USER -d $DB_NAME << EOF
INSERT INTO analytics.deployment_log (deployment_date, git_commit, deployed_by, status)
VALUES (NOW(), '${BITBUCKET_COMMIT}', '${BITBUCKET_BUILD_NUMBER}', 'SUCCESS');
EOF

echo "Deployment completed successfully at $(date)" | tee -a $DEPLOYMENT_LOG
