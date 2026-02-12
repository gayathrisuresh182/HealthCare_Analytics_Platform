-- Custom SQL for Tableau - Readmissions Dashboard Dataset
-- This query joins readmissions fact with hospitals and geography
-- Use this in Tableau: Connect → Snowflake → New Custom SQL

SELECT 
    -- Readmission fact fields
    r.readmission_key,
    r.hospital_key,
    r.geography_key,
    r.start_date_key,
    r.end_date_key,
    
    -- Readmission measures
    r.measure_name,
    r.measure_category,
    r.number_of_discharges,
    r.number_of_readmissions,
    r.excess_readmission_ratio,
    r.predicted_readmission_rate,
    r.expected_readmission_rate,
    r.observed_readmission_rate,
    r.performance_category,
    
    -- Dates
    r.start_date,
    r.end_date,
    r.footnote,
    
    -- Hospital dimension fields
    h.facility_id,
    h.facility_name,
    h.address,
    h.city,
    h.state AS hospital_state,
    h.zip_code,
    h.county,
    h.hospital_type,
    h.hospital_ownership,
    h.emergency_services,
    h.birthing_friendly,
    h.hospital_overall_rating,
    h.rating_footnote,
    
    -- Quality measure counts
    h.mort_measures_better,
    h.mort_measures_no_different,
    h.mort_measures_worse,
    h.safety_measures_better,
    h.safety_measures_no_different,
    h.safety_measures_worse,
    h.readm_measures_better,
    h.readm_measures_no_different,
    h.readm_measures_worse,
    
    -- Geography dimension fields
    g.state_abbreviation,
    g.county AS geography_county,
    g.city AS geography_city,
    g.urban_rural_classification,
    g.census_region,
    g.census_division,
    
    -- Calculated fields (for Tableau)
    CASE 
        WHEN r.excess_readmission_ratio < 0.9 THEN "Excellent"
        WHEN r.excess_readmission_ratio < 1.0 THEN "Good"
        WHEN r.excess_readmission_ratio < 1.1 THEN "Average"
        ELSE "Needs Improvement"
    END AS performance_tier,
    
    CASE 
        WHEN r.excess_readmission_ratio < 1.0 THEN TRUE 
        ELSE FALSE 
    END AS is_better_than_expected,
    
    CASE 
        WHEN r.number_of_discharges >= 25 THEN TRUE 
        ELSE FALSE 
    END AS has_sufficient_volume

FROM raw_marts.fct_readmissions r

-- Join to hospitals (only current)
INNER JOIN raw_marts.dim_hospitals h
    ON r.hospital_key = h.hospital_key
    AND h.is_current = TRUE  -- Filter to current hospitals only

-- Join to geography
INNER JOIN raw_marts.dim_geography g
    ON r.geography_key = g.geography_key

-- Optional: Add filters here if needed
-- WHERE r.excess_readmission_ratio IS NOT NULL
-- AND r.number_of_discharges > 0

