{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging model for Hospital Readmissions Reduction Program
-- Source: raw.readmissions
-- Grain: One row per hospital per readmission measure
-- Purpose: Clean column names, convert data types, parse dates

WITH source AS (
    SELECT * FROM {{ source('raw', 'readmissions') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        "Facility ID" AS facility_id,
        "Facility Name" AS facility_name,
        "State" AS state,
        
        -- Measure information
        "Measure Name" AS measure_name,
        
        -- Volume
        CAST(NULLIF(NULLIF(TRIM("Number of Discharges"), ''), 'N/A') AS INTEGER) AS number_of_discharges,
        CAST(NULLIF(NULLIF(TRIM("Number of Readmissions"), ''), 'N/A') AS INTEGER) AS number_of_readmissions,
        
        -- Quality metrics
        CAST(NULLIF(NULLIF(TRIM("Excess Readmission Ratio"), ''), 'N/A') AS DECIMAL(10, 4)) AS excess_readmission_ratio,
        CAST(NULLIF(NULLIF(TRIM("Predicted Readmission Rate"), ''), 'N/A') AS DECIMAL(10, 4)) AS predicted_readmission_rate,
        CAST(NULLIF(NULLIF(TRIM("Expected Readmission Rate"), ''), 'N/A') AS DECIMAL(10, 4)) AS expected_readmission_rate,
        
        -- Dates
        TRY_TO_DATE("Start Date", 'MM/DD/YYYY') AS start_date,
        TRY_TO_DATE("End Date", 'MM/DD/YYYY') AS end_date,
        
        -- Footnotes
        "Footnote" AS footnote
        
    FROM source
    WHERE "Facility ID" IS NOT NULL
      AND "Measure Name" IS NOT NULL
)

SELECT * FROM cleaned

