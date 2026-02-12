{{
    config(
        materialized='view',
        schema='staging'
    )
}}

-- Staging model for Hospital General Information
-- Source: raw.hospital_general_info
-- Grain: One row per hospital (master list)
-- Purpose: Clean column names, standardize categorical fields, handle nulls

WITH source AS (
    SELECT * FROM {{ source('raw', 'hospital_general_info') }}
),

cleaned AS (
    SELECT
        -- Primary key
        "Facility ID" AS facility_id,
        
        -- Hospital identifiers
        "Facility Name" AS facility_name,
        
        -- Geographic information
        "Address" AS address,
        "City/Town" AS city,
        "State" AS state,
        "ZIP Code" AS zip_code,
        "County/Parish" AS county,
        "Telephone Number" AS telephone_number,
        
        -- Hospital characteristics
        "Hospital Type" AS hospital_type,
        -- Standardize hospital ownership values
        CASE 
            WHEN UPPER(TRIM("Hospital Ownership")) LIKE '%GOVERNMENT%' OR UPPER(TRIM("Hospital Ownership")) LIKE '%FEDERAL%' OR UPPER(TRIM("Hospital Ownership")) LIKE '%STATE%' OR UPPER(TRIM("Hospital Ownership")) LIKE '%LOCAL%' THEN 'Government'
            WHEN UPPER(TRIM("Hospital Ownership")) LIKE '%PROPRIETARY%' OR UPPER(TRIM("Hospital Ownership")) LIKE '%PROFIT%' THEN 'Proprietary'
            WHEN UPPER(TRIM("Hospital Ownership")) LIKE '%VOLUNTARY%' OR UPPER(TRIM("Hospital Ownership")) LIKE '%NON-PROFIT%' OR UPPER(TRIM("Hospital Ownership")) LIKE '%NONPROFIT%' THEN 'Voluntary non-profit'
            WHEN UPPER(TRIM("Hospital Ownership")) LIKE '%PHYSICIAN%' THEN 'Physician'
            WHEN UPPER(TRIM("Hospital Ownership")) LIKE '%TRIBAL%' THEN 'Tribal'
            WHEN TRIM("Hospital Ownership") IN ('Government', 'Proprietary', 'Voluntary non-profit', 'Physician', 'Tribal') THEN TRIM("Hospital Ownership")
            ELSE 'Unknown'
        END AS hospital_ownership,
        
        -- Services
        CASE 
            WHEN UPPER(TRIM("Emergency Services")) = 'YES' THEN 'Yes'
            WHEN UPPER(TRIM("Emergency Services")) = 'NO' THEN 'No'
            ELSE NULL
        END AS emergency_services,
        
        CASE 
            WHEN UPPER(TRIM("Meets criteria for birthing friendly designation")) = 'Y' THEN 'Yes'
            WHEN UPPER(TRIM("Meets criteria for birthing friendly designation")) = 'N' THEN 'No'
            ELSE NULL
        END AS birthing_friendly,
        
        -- Quality ratings
        CASE 
            WHEN TRIM("Hospital overall rating") IN ('1', '2', '3', '4', '5') 
            THEN CAST(TRIM("Hospital overall rating") AS INTEGER)
            ELSE NULL
        END AS hospital_overall_rating,
        
        "Hospital overall rating footnote" AS rating_footnote,
        
        -- Mortality measures
        CAST(NULLIF(TRIM("MORT Group Measure Count"), '') AS INTEGER) AS mort_group_measure_count,
        CAST(NULLIF(TRIM("Count of Facility MORT Measures"), '') AS INTEGER) AS facility_mort_measures,
        CAST(NULLIF(TRIM("Count of MORT Measures Better"), '') AS INTEGER) AS mort_measures_better,
        CAST(NULLIF(TRIM("Count of MORT Measures No Different"), '') AS INTEGER) AS mort_measures_no_different,
        CAST(NULLIF(TRIM("Count of MORT Measures Worse"), '') AS INTEGER) AS mort_measures_worse,
        "MORT Group Footnote" AS mort_footnote,
        
        -- Safety measures
        CAST(NULLIF(TRIM("Safety Group Measure Count"), '') AS INTEGER) AS safety_group_measure_count,
        CAST(NULLIF(TRIM("Count of Facility Safety Measures"), '') AS INTEGER) AS facility_safety_measures,
        CAST(NULLIF(TRIM("Count of Safety Measures Better"), '') AS INTEGER) AS safety_measures_better,
        CAST(NULLIF(TRIM("Count of Safety Measures No Different"), '') AS INTEGER) AS safety_measures_no_different,
        CAST(NULLIF(TRIM("Count of Safety Measures Worse"), '') AS INTEGER) AS safety_measures_worse,
        "Safety Group Footnote" AS safety_footnote,
        
        -- Readmission measures
        CAST(NULLIF(TRIM("READM Group Measure Count"), '') AS INTEGER) AS readm_group_measure_count,
        CAST(NULLIF(TRIM("Count of Facility READM Measures"), '') AS INTEGER) AS facility_readm_measures,
        CAST(NULLIF(TRIM("Count of READM Measures Better"), '') AS INTEGER) AS readm_measures_better,
        CAST(NULLIF(TRIM("Count of READM Measures No Different"), '') AS INTEGER) AS readm_measures_no_different,
        CAST(NULLIF(TRIM("Count of READM Measures Worse"), '') AS INTEGER) AS readm_measures_worse,
        "READM Group Footnote" AS readm_footnote,
        
        -- Patient experience measures
        CAST(NULLIF(TRIM("Pt Exp Group Measure Count"), '') AS INTEGER) AS pt_exp_group_measure_count,
        CAST(NULLIF(TRIM("Count of Facility Pt Exp Measures"), '') AS INTEGER) AS facility_pt_exp_measures,
        "Pt Exp Group Footnote" AS pt_exp_footnote,
        
        -- Timely and effective care measures
        CAST(NULLIF(TRIM("TE Group Measure Count"), '') AS INTEGER) AS te_group_measure_count,
        CAST(NULLIF(TRIM("Count of Facility TE Measures"), '') AS INTEGER) AS facility_te_measures,
        "TE Group Footnote" AS te_footnote
        
    FROM source
    WHERE "Facility ID" IS NOT NULL
)

SELECT * FROM cleaned

