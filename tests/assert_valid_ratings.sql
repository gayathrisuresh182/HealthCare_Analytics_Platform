-- Custom test: Assert that hospital ratings are valid (1-5 or NULL)
-- Purpose: Data quality check for rating values

SELECT 
    hospital_key,
    facility_id,
    hospital_overall_rating
FROM {{ ref('dim_hospitals') }}
WHERE hospital_overall_rating IS NOT NULL
  AND (hospital_overall_rating < 1 OR hospital_overall_rating > 5)

