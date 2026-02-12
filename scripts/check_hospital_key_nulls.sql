-- Query to investigate NULL hospital_key values in fct_inpatient_charges
-- Run this in Snowflake to see which hospital_id values don't match

-- Count NULL hospital_key values
SELECT 
    COUNT(*) AS total_rows,
    COUNT(hospital_key) AS rows_with_hospital_key,
    COUNT(*) - COUNT(hospital_key) AS rows_with_null_hospital_key,
    ROUND((COUNT(*) - COUNT(hospital_key)) * 100.0 / COUNT(*), 2) AS pct_null
FROM raw_marts.fct_inpatient_charges;

-- Sample rows with NULL hospital_key
SELECT 
    hospital_id,
    drg_code,
    charge_key,
    total_discharges,
    avg_covered_charges
FROM raw_marts.fct_inpatient_charges
WHERE hospital_key IS NULL
LIMIT 10;

-- Check if these hospital_ids exist in dim_hospitals
SELECT DISTINCT
    c.hospital_id,
    CASE 
        WHEN h.facility_id IS NOT NULL THEN 'EXISTS in dim_hospitals'
        ELSE 'MISSING from dim_hospitals'
    END AS status
FROM raw_marts.fct_inpatient_charges c
LEFT JOIN raw_marts.dim_hospitals h
    ON c.hospital_id = h.facility_id
    AND h.is_current = TRUE
WHERE c.hospital_key IS NULL
GROUP BY c.hospital_id, h.facility_id
LIMIT 20;

