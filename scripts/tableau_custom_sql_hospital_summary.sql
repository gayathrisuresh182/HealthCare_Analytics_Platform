-- Custom SQL for Tableau - Hospital Summary (Pre-aggregated)
-- Use this for hospital-level dashboards (faster performance)
-- Use this in Tableau: Connect → Snowflake → New Custom SQL

SELECT 
    -- Hospital identifiers
    h.hospital_key,
    h.facility_id,
    h.facility_name,
    h.address,
    h.city,
    h.state,
    h.zip_code,
    h.county,
    h.telephone_number,
    
    -- Hospital characteristics
    h.hospital_type,
    h.hospital_ownership,
    h.emergency_services,
    h.birthing_friendly,
    h.hospital_overall_rating,
    h.rating_footnote,
    
    -- Quality measure counts
    h.mort_group_measure_count,
    h.facility_mort_measures,
    h.mort_measures_better,
    h.mort_measures_no_different,
    h.mort_measures_worse,
    h.safety_group_measure_count,
    h.facility_safety_measures,
    h.safety_measures_better,
    h.safety_measures_no_different,
    h.safety_measures_worse,
    h.readm_group_measure_count,
    h.facility_readm_measures,
    h.readm_measures_better,
    h.readm_measures_no_different,
    h.readm_measures_worse,
    
    -- Aggregated charge metrics
    COUNT(DISTINCT c.charge_key) AS total_charge_records,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_covered_charges,
    SUM(c.total_payments) AS total_payments,
    SUM(c.total_medicare_payments) AS total_medicare_payments,
    AVG(c.avg_covered_charges) AS avg_charge_per_discharge,
    AVG(c.avg_total_payment) AS avg_total_payment,
    AVG(c.avg_medicare_payment) AS avg_medicare_payment,
    AVG(c.markup_ratio) AS avg_markup_ratio,
    
    -- Data quality metrics
    SUM(CASE WHEN c.has_data_quality_issues THEN 1 ELSE 0 END) AS records_with_quality_issues,
    SUM(CASE WHEN c.has_orphaned_hospital THEN 1 ELSE 0 END) AS orphaned_records,
    ROUND(SUM(CASE WHEN c.has_data_quality_issues THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS quality_issue_percentage,
    
    -- DRG diversity
    COUNT(DISTINCT c.drg_key) AS distinct_drg_count,
    
    -- Geography
    MAX(g.state_name) AS state_name,
    MAX(g.urban_rural_classification) AS urban_rural_classification

FROM raw_marts.fct_inpatient_charges c

INNER JOIN raw_marts.dim_hospitals h
    ON c.hospital_key = h.hospital_key
    AND h.is_current = TRUE

LEFT JOIN raw_marts.dim_geography g
    ON c.geography_key = g.geography_key

WHERE c.hospital_key IS NOT NULL

GROUP BY 
    h.hospital_key,
    h.facility_id,
    h.facility_name,
    h.address,
    h.city,
    h.state,
    h.zip_code,
    h.county,
    h.telephone_number,
    h.hospital_type,
    h.hospital_ownership,
    h.emergency_services,
    h.birthing_friendly,
    h.hospital_overall_rating,
    h.rating_footnote,
    h.mort_group_measure_count,
    h.facility_mort_measures,
    h.mort_measures_better,
    h.mort_measures_no_different,
    h.mort_measures_worse,
    h.safety_group_measure_count,
    h.facility_safety_measures,
    h.safety_measures_better,
    h.safety_measures_no_different,
    h.safety_measures_worse,
    h.readm_group_measure_count,
    h.facility_readm_measures,
    h.readm_measures_better,
    h.readm_measures_no_different,
    h.readm_measures_worse

ORDER BY total_covered_charges DESC

