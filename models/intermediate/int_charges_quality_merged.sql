{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

-- Intermediate model: Merging charges data with hospital quality metrics
-- Demonstrates: Complex LEFT JOINs, COALESCE for handling unmatched records
-- Purpose: Create unified dataset of charges + quality for analysis

WITH charges AS (
    SELECT * FROM {{ ref('stg_ipps_charges') }}
),

hospitals AS (
    SELECT * FROM {{ ref('stg_hospitals') }}
),

merged AS (
    SELECT
        -- Hospital identifiers
        COALESCE(c.hospital_id, h.facility_id) AS hospital_id,
        COALESCE(c.hospital_name, h.facility_name) AS hospital_name,
        
        -- Geographic information (prefer charges, fallback to hospitals)
        COALESCE(c.city, h.city) AS city,
        COALESCE(c.state_abbreviation, h.state) AS state,
        COALESCE(c.zip_code, h.zip_code) AS zip_code,
        h.county,
        c.ruca_code,
        c.ruca_description,
        
        -- DRG information
        c.drg_code,
        c.drg_description,
        
        -- Charge metrics
        c.total_discharges,
        c.avg_covered_charges,
        c.avg_total_payment,
        c.avg_medicare_payment,
        
        -- Quality metrics from hospitals table
        h.hospital_type,
        h.hospital_ownership,
        h.hospital_overall_rating,
        h.emergency_services,
        h.facility_mort_measures,
        h.mort_measures_better,
        h.mort_measures_worse,
        h.facility_safety_measures,
        h.safety_measures_better,
        h.safety_measures_worse,
        h.facility_readm_measures,
        h.readm_measures_better,
        h.readm_measures_worse
        
    FROM charges c
    LEFT JOIN hospitals h
        ON c.hospital_id = h.facility_id
)

SELECT * FROM merged

