{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging model for Medicare Inpatient Hospitals - by Provider and Service
-- Source: raw.ipps_charges
-- Grain: One row per hospital per DRG procedure code
-- Purpose: Clean column names, remove formatting from currency fields, convert data types

WITH source AS (
    SELECT * FROM {{ source('raw', 'ipps_charges') }}
),

cleaned AS (
    SELECT
        -- Primary keys and identifiers
        Rndrng_Prvdr_CCN AS hospital_id,
        Rndrng_Prvdr_Org_Name AS hospital_name,
        
        -- Geographic information
        Rndrng_Prvdr_City AS city,
        Rndrng_Prvdr_St AS street_address,
        Rndrng_Prvdr_State_FIPS AS state_fips_code,
        Rndrng_Prvdr_Zip5 AS zip_code,
        Rndrng_Prvdr_State_Abrvtn AS state_abbreviation,
        Rndrng_Prvdr_RUCA AS ruca_code,
        Rndrng_Prvdr_RUCA_Desc AS ruca_description,
        
        -- DRG information
        DRG_Cd AS drg_code,
        DRG_Desc AS drg_description,
        
        -- Volume and financial measures
        -- Remove $ and commas, convert to DECIMAL
        CAST(
            REPLACE(
                REPLACE(
                    NULLIF(TRIM(Tot_Dschrgs), ''),
                    ',',
                    ''
                ),
                '$',
                ''
            ) AS INTEGER
        ) AS total_discharges,
        
        -- Preserve original values in staging (capping applied in marts layer)
        CAST(
            REPLACE(
                REPLACE(
                    NULLIF(TRIM(Avg_Submtd_Cvrd_Chrg), ''),
                    ',',
                    ''
                ),
                '$',
                ''
            ) AS DECIMAL(18, 2)
        ) AS avg_covered_charges,
        
        CAST(
            REPLACE(
                REPLACE(
                    NULLIF(TRIM(Avg_Tot_Pymt_Amt), ''),
                    ',',
                    ''
                ),
                '$',
                ''
            ) AS DECIMAL(18, 2)
        ) AS avg_total_payment,
        
        CAST(
            REPLACE(
                REPLACE(
                    NULLIF(TRIM(Avg_Mdcr_Pymt_Amt), ''),
                    ',',
                    ''
                ),
                '$',
                ''
            ) AS DECIMAL(18, 2)
        ) AS avg_medicare_payment
        
    FROM source
    WHERE Rndrng_Prvdr_CCN IS NOT NULL
      AND DRG_Cd IS NOT NULL
)

SELECT * FROM cleaned

