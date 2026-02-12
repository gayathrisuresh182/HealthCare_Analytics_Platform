{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Fact: Readmissions (Detail Level)
-- Grain: One row per hospital per readmission measure
-- Source: stg_readmissions
-- Purpose: Detail fact table for readmission analysis

WITH readmissions_raw AS (
    SELECT * FROM {{ ref('stg_readmissions') }}
),

-- Deduplicate: If same facility_id, measure_name, start_date, end_date exist multiple times,
-- keep the one with highest number_of_discharges (most complete data)
-- Use COALESCE for NULL dates to ensure proper partitioning
readmissions_deduped AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY 
                facility_id, 
                measure_name, 
                COALESCE(start_date, '1900-01-01'::DATE), 
                COALESCE(end_date, '1900-01-01'::DATE)
            ORDER BY number_of_discharges DESC NULLS LAST, number_of_readmissions DESC NULLS LAST
        ) AS row_num
    FROM readmissions_raw
    WHERE excess_readmission_ratio IS NOT NULL
      AND number_of_discharges > 0
),

readmissions AS (
    SELECT 
        facility_id,
        facility_name,
        state,
        measure_name,
        number_of_discharges,
        number_of_readmissions,
        excess_readmission_ratio,
        predicted_readmission_rate,
        expected_readmission_rate,
        start_date,
        end_date,
        footnote,
        -- Generate a truly unique sequential ID to handle any remaining duplicates
        -- This ensures uniqueness even when all business key fields are identical
        ROW_NUMBER() OVER (
            ORDER BY 
                facility_id, 
                measure_name, 
                COALESCE(start_date, '1900-01-01'::DATE), 
                COALESCE(end_date, '1900-01-01'::DATE),
                state,
                number_of_discharges DESC,
                number_of_readmissions DESC,
                excess_readmission_ratio,
                predicted_readmission_rate,
                expected_readmission_rate
        ) AS unique_sequence_id
    FROM readmissions_deduped
    WHERE row_num = 1
),

hospitals AS (
    SELECT hospital_key, facility_id FROM {{ ref('dim_hospitals') }}
    WHERE is_current = TRUE
),

geography AS (
    SELECT 
        state_abbreviation,
        FIRST_VALUE(geography_key) OVER (PARTITION BY state_abbreviation ORDER BY geography_key) AS geography_key
    FROM {{ ref('dim_geography') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbreviation ORDER BY geography_key) = 1
),

dates AS (
    SELECT date_key, date_day 
    FROM {{ ref('dim_dates') }}
),

joined_data AS (
    SELECT
        r.unique_sequence_id,
        r.facility_id,
        r.facility_name,
        r.state,
        r.measure_name,
        r.number_of_discharges,
        r.number_of_readmissions,
        r.excess_readmission_ratio,
        r.predicted_readmission_rate,
        r.expected_readmission_rate,
        r.start_date,
        r.end_date,
        r.footnote,
        h.hospital_key,
        g.geography_key,
        d_start.date_key AS start_date_key,
        d_end.date_key AS end_date_key
    FROM readmissions r
    LEFT JOIN hospitals h
        ON r.facility_id = h.facility_id
    LEFT JOIN geography g
        ON r.state = g.state_abbreviation
    LEFT JOIN dates d_start
        ON r.start_date = d_start.date_day
    LEFT JOIN dates d_end
        ON r.end_date = d_end.date_day
)

SELECT
    -- Surrogate keys - calculated after joins to ensure uniqueness
    {{ dbt_utils.generate_surrogate_key(['j.facility_id', 'j.measure_name', 'COALESCE(CAST(j.start_date AS VARCHAR), \'NULL\')', 'COALESCE(CAST(j.end_date AS VARCHAR), \'NULL\')', 'CAST(j.unique_sequence_id AS VARCHAR)']) }} AS readmission_key,
    j.hospital_key,
    j.geography_key,
    j.start_date_key,
    j.end_date_key,
    
    -- Degenerate dimensions
    j.facility_id,
    j.measure_name,
    
    -- Measure categorization
    CASE 
        WHEN j.measure_name LIKE '%AMI%' THEN 'Acute Myocardial Infarction'
        WHEN j.measure_name LIKE '%HF%' THEN 'Heart Failure'
        WHEN j.measure_name LIKE '%PN%' THEN 'Pneumonia'
        WHEN j.measure_name LIKE '%COPD%' THEN 'COPD'
        WHEN j.measure_name LIKE '%CABG%' THEN 'Coronary Artery Bypass Graft'
        WHEN j.measure_name LIKE '%THA%' OR j.measure_name LIKE '%TKA%' THEN 'Hip/Knee Replacement'
        ELSE 'Other'
    END AS measure_category,
    
    -- Measures
    j.number_of_discharges,
    j.number_of_readmissions,
    j.excess_readmission_ratio,
    j.predicted_readmission_rate,
    j.expected_readmission_rate,
    
    -- Calculated measures
    CASE 
        WHEN j.number_of_discharges > 0 
        THEN j.number_of_readmissions / j.number_of_discharges 
        ELSE NULL 
    END AS observed_readmission_rate,
    
    -- Performance indicators
    CASE 
        WHEN j.excess_readmission_ratio < 1.0 THEN 'Better than Expected'
        WHEN j.excess_readmission_ratio = 1.0 THEN 'As Expected'
        WHEN j.excess_readmission_ratio > 1.0 THEN 'Worse than Expected'
        ELSE 'Unknown'
    END AS performance_category,
    
    -- Dates
    j.start_date,
    j.end_date,
    j.footnote

FROM joined_data j
QUALIFY ROW_NUMBER() OVER (PARTITION BY j.unique_sequence_id ORDER BY j.hospital_key, j.geography_key) = 1

