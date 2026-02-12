{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Dimension: Hospitals (with SCD Type 2 support)
-- Purpose: Hospital master dimension with current and historical versions
-- Source: stg_hospitals (initial) + hospitals_snapshot (for SCD Type 2 tracking once created)
-- Note: Initially uses stg_hospitals directly. Once snapshot is created, can switch to snapshot.

WITH current_hospitals AS (
    SELECT * FROM {{ ref('stg_hospitals') }}
),

-- Try to use snapshot if it exists, otherwise use staging
-- Note: This will use staging until snapshot is created
snapshot_data AS (
    SELECT * FROM current_hospitals
),

enriched AS (
    SELECT
        -- Generate hospital_key based on facility_id (consistent across versions)
        {{ dbt_utils.generate_surrogate_key(['facility_id']) }} AS hospital_key,
        
        -- Generate SCD ID (will use snapshot's dbt_scd_id once snapshot is created)
        {{ dbt_utils.generate_surrogate_key(['facility_id', 'hospital_ownership', 'hospital_overall_rating', 'emergency_services']) }} AS scd_id,
        
        -- Hospital identifiers
        facility_id,
        facility_name,
        address,
        city,
        state,
        zip_code,
        county,
        telephone_number,
        
        -- Hospital characteristics
        hospital_type,
        hospital_ownership,
        emergency_services,
        birthing_friendly,
        hospital_overall_rating,
        rating_footnote,
        
        -- Quality measure counts
        mort_group_measure_count,
        facility_mort_measures,
        mort_measures_better,
        mort_measures_no_different,
        mort_measures_worse,
        safety_group_measure_count,
        facility_safety_measures,
        safety_measures_better,
        safety_measures_no_different,
        safety_measures_worse,
        readm_group_measure_count,
        facility_readm_measures,
        readm_measures_better,
        readm_measures_no_different,
        readm_measures_worse,
        pt_exp_group_measure_count,
        facility_pt_exp_measures,
        te_group_measure_count,
        facility_te_measures,
        
        -- SCD Type 2 fields
        -- Initially set all as current (will use snapshot values once snapshot is created)
        CURRENT_TIMESTAMP() AS valid_from,
        CAST('9999-12-31' AS TIMESTAMP_NTZ) AS valid_to,
        TRUE AS is_current
        
    FROM snapshot_data
)

SELECT * FROM enriched

