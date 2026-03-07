-- sql/01_staging/stg_accounts.sql
-- Description: Cleans account demographic and commercial attributes
-- Depends on: raw_data.accounts_raw

DROP TABLE IF EXISTS staging.stg_accounts;

CREATE TABLE staging.stg_accounts AS
WITH source AS (
    SELECT *
    FROM raw_data.accounts_raw
    WHERE load_date >= CURRENT_DATE - INTERVAL '365 days'
),

cleaned AS (
    SELECT
        account_id,
        account_name,
        industry,
        territory,
        arr_band,
        employee_band,
        customer_tier,

        -- Business flags
        CASE WHEN arr_band IN ('250K-1M', '1M+') THEN 1 ELSE 0 END AS is_strategic_account,
        CASE WHEN customer_tier = 'Customer' THEN 1 ELSE 0 END AS is_customer,

        -- KPI calculations
        CASE WHEN territory IN ('NA-East', 'NA-West') THEN 'North America' ELSE 'International' END AS region_rollup,

        -- Metadata
        load_date AS _loaded_at,
        CURRENT_TIMESTAMP AS _transformed_at
    FROM source
    WHERE account_id IS NOT NULL
)

SELECT * FROM cleaned;

CREATE INDEX idx_stg_accounts_account_id ON staging.stg_accounts(account_id);
CREATE INDEX idx_stg_accounts_industry ON staging.stg_accounts(industry);
CREATE INDEX idx_stg_accounts_territory ON staging.stg_accounts(territory);
CREATE INDEX idx_stg_accounts_arr_band ON staging.stg_accounts(arr_band);
