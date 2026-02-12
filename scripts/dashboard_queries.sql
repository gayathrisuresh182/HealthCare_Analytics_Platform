-- Healthcare Analytics Dashboard Queries
-- Use these queries in your BI tool (Power BI, Tableau, etc.)

-- ============================================================
-- 1. EXECUTIVE SUMMARY DASHBOARD
-- ============================================================

-- Overall KPIs
SELECT 
    COUNT(DISTINCT c.charge_key) AS total_charge_records,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_charges,
    AVG(c.avg_covered_charges) AS avg_charge_per_discharge,
    AVG(c.markup_ratio) AS avg_markup_ratio,
    COUNT(DISTINCT c.hospital_key) AS hospital_count,
    COUNT(DISTINCT c.drg_key) AS drg_count,
    SUM(CASE WHEN c.has_data_quality_issues THEN 1 ELSE 0 END) AS records_with_quality_issues,
    ROUND(SUM(CASE WHEN c.has_data_quality_issues THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS quality_issue_percentage
FROM raw_marts.fct_inpatient_charges c;

-- ============================================================
-- 2. HOSPITAL PERFORMANCE DASHBOARD
-- ============================================================

-- Top Hospitals by Charges
SELECT 
    h.facility_name,
    h.state,
    h.hospital_ownership,
    h.hospital_overall_rating,
    COUNT(DISTINCT c.charge_key) AS charge_records,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_charges,
    AVG(c.avg_covered_charges) AS avg_charge_per_discharge,
    AVG(c.markup_ratio) AS avg_markup_ratio,
    SUM(CASE WHEN c.has_orphaned_hospital THEN 1 ELSE 0 END) AS orphaned_records
FROM raw_marts.fct_inpatient_charges c
LEFT JOIN raw_marts.dim_hospitals h
    ON c.hospital_key = h.hospital_key
    AND h.is_current = TRUE
WHERE c.hospital_key IS NOT NULL
GROUP BY 
    h.facility_name,
    h.state,
    h.hospital_ownership,
    h.hospital_overall_rating
ORDER BY total_charges DESC
LIMIT 50;

-- Hospital Performance by Ownership Type
SELECT 
    h.hospital_ownership,
    COUNT(DISTINCT h.hospital_key) AS hospital_count,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_charges,
    AVG(c.avg_covered_charges) AS avg_charge_per_discharge,
    AVG(c.markup_ratio) AS avg_markup_ratio,
    AVG(h.hospital_overall_rating) AS avg_rating
FROM raw_marts.fct_inpatient_charges c
JOIN raw_marts.dim_hospitals h
    ON c.hospital_key = h.hospital_key
    AND h.is_current = TRUE
WHERE c.hospital_key IS NOT NULL
GROUP BY h.hospital_ownership
ORDER BY total_charges DESC;

-- ============================================================
-- 3. COST ANALYSIS DASHBOARD
-- ============================================================

-- Top DRGs by Charges
SELECT 
    d.drg_code,
    d.drg_description,
    d.drg_category,
    COUNT(DISTINCT c.charge_key) AS charge_records,
    SUM(c.total_discharges) AS total_discharges,
    AVG(c.avg_covered_charges) AS avg_charge,
    AVG(c.avg_medicare_payment) AS avg_medicare_payment,
    AVG(c.markup_ratio) AS avg_markup_ratio,
    SUM(c.total_covered_charges) AS total_charges,
    SUM(c.total_medicare_payments) AS total_medicare_payments
FROM raw_marts.fct_inpatient_charges c
JOIN raw_marts.dim_drg_codes d
    ON c.drg_key = d.drg_key
GROUP BY 
    d.drg_code,
    d.drg_description,
    d.drg_category
ORDER BY total_charges DESC
LIMIT 20;

-- Cost Distribution by DRG Category
SELECT 
    d.drg_category,
    COUNT(DISTINCT d.drg_code) AS drg_count,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_charges,
    AVG(c.avg_covered_charges) AS avg_charge,
    AVG(c.markup_ratio) AS avg_markup_ratio
FROM raw_marts.fct_inpatient_charges c
JOIN raw_marts.dim_drg_codes d
    ON c.drg_key = d.drg_key
GROUP BY d.drg_category
ORDER BY total_charges DESC;

-- ============================================================
-- 4. GEOGRAPHIC ANALYSIS DASHBOARD
-- ============================================================

-- State-Level Summary
SELECT 
    g.state_abbreviation,
    g.state_name,
    COUNT(DISTINCT c.hospital_key) AS hospital_count,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_charges,
    AVG(c.avg_covered_charges) AS avg_charge_per_discharge,
    AVG(c.markup_ratio) AS avg_markup_ratio
FROM raw_marts.fct_inpatient_charges c
JOIN raw_marts.dim_geography g
    ON c.geography_key = g.geography_key
WHERE c.hospital_key IS NOT NULL
GROUP BY 
    g.state_abbreviation,
    g.state_name
ORDER BY total_charges DESC;

-- Urban vs Rural Analysis
SELECT 
    g.urban_rural_classification,
    COUNT(DISTINCT c.hospital_key) AS hospital_count,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_charges,
    AVG(c.avg_covered_charges) AS avg_charge_per_discharge,
    AVG(c.markup_ratio) AS avg_markup_ratio
FROM raw_marts.fct_inpatient_charges c
JOIN raw_marts.dim_geography g
    ON c.geography_key = g.geography_key
WHERE c.hospital_key IS NOT NULL
GROUP BY g.urban_rural_classification
ORDER BY total_charges DESC;

-- ============================================================
-- 5. QUALITY METRICS DASHBOARD
-- ============================================================

-- Hospital Quality vs Cost
SELECT 
    h.hospital_overall_rating,
    COUNT(DISTINCT h.hospital_key) AS hospital_count,
    AVG(c.avg_covered_charges) AS avg_charge,
    AVG(c.markup_ratio) AS avg_markup_ratio,
    AVG(r.excess_readmission_ratio) AS avg_excess_readmission_ratio
FROM raw_marts.dim_hospitals h
LEFT JOIN raw_marts.fct_inpatient_charges c
    ON h.hospital_key = c.hospital_key
    AND h.is_current = TRUE
LEFT JOIN raw_marts.fct_readmissions r
    ON h.hospital_key = r.hospital_key
WHERE h.is_current = TRUE
    AND c.hospital_key IS NOT NULL
GROUP BY h.hospital_overall_rating
ORDER BY h.hospital_overall_rating DESC;

-- Readmission Analysis
SELECT 
    h.facility_name,
    h.state,
    h.hospital_overall_rating,
    SUM(r.number_of_discharges) AS total_discharges,
    SUM(r.number_of_readmissions) AS total_readmissions,
    AVG(r.excess_readmission_ratio) AS avg_excess_readmission_ratio,
    CASE 
        WHEN SUM(r.number_of_discharges) > 0 
        THEN SUM(r.number_of_readmissions) * 100.0 / SUM(r.number_of_discharges)
        ELSE NULL
    END AS readmission_rate
FROM raw_marts.fct_readmissions r
JOIN raw_marts.dim_hospitals h
    ON r.hospital_key = h.hospital_key
    AND h.is_current = TRUE
GROUP BY 
    h.facility_name,
    h.state,
    h.hospital_overall_rating
HAVING SUM(r.number_of_discharges) > 0
ORDER BY readmission_rate DESC
LIMIT 50;

-- ============================================================
-- 6. DATA QUALITY DASHBOARD
-- ============================================================

-- Data Quality Summary
SELECT 
    COUNT(*) AS total_records,
    SUM(CASE WHEN has_data_quality_issues THEN 1 ELSE 0 END) AS records_with_issues,
    SUM(CASE WHEN has_orphaned_hospital THEN 1 ELSE 0 END) AS orphaned_records,
    SUM(CASE WHEN data_quality_flag_covered_charges_issue THEN 1 ELSE 0 END) AS covered_charges_issues,
    SUM(CASE WHEN data_quality_flag_capped_charges THEN 1 ELSE 0 END) AS capped_charges,
    SUM(CASE WHEN data_quality_flag_capped_markup_ratio THEN 1 ELSE 0 END) AS capped_markup_ratios,
    ROUND(SUM(CASE WHEN has_data_quality_issues THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS quality_issue_percentage
FROM raw_marts.fct_inpatient_charges;

-- Orphaned Hospitals Analysis
SELECT 
    hospital_id,
    COUNT(*) AS charge_records,
    SUM(total_discharges) AS total_discharges,
    SUM(total_covered_charges) AS total_charges,
    AVG(avg_covered_charges) AS avg_charge
FROM raw_marts.fct_inpatient_charges
WHERE has_orphaned_hospital = TRUE
GROUP BY hospital_id
ORDER BY charge_records DESC
LIMIT 20;

-- ============================================================
-- 7. TREND ANALYSIS (if you have date dimension)
-- ============================================================

-- Monthly Trends (if date data available)
-- Note: Adjust based on your date fields
SELECT 
    DATE_TRUNC('MONTH', CURRENT_DATE()) AS month,  -- Replace with actual date field
    COUNT(DISTINCT c.charge_key) AS charge_records,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_charges,
    AVG(c.avg_covered_charges) AS avg_charge
FROM raw_marts.fct_inpatient_charges c
GROUP BY DATE_TRUNC('MONTH', CURRENT_DATE())
ORDER BY month DESC;

