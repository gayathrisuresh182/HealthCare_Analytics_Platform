{{
    config(
        target_schema='snapshots',
        unique_key='facility_id',
        strategy='check',
        check_cols=['hospital_ownership', 'hospital_overall_rating', 'emergency_services'],
        invalidate_hard_deletes=True
    )
}}

-- Snapshot: Hospitals SCD Type 2
-- Purpose: Track historical changes to hospital attributes
-- Strategy: Check columns for changes in ownership, rating, and emergency services

SELECT * FROM {{ ref('stg_hospitals') }}

