-- Custom SQL for Tableau - DRG Summary (Pre-aggregated)
-- Use this for DRG-level analysis dashboards
-- Use this in Tableau: Connect → Snowflake → New Custom SQL

SELECT 
    -- DRG identifiers
    d.drg_key,
    d.drg_code,
    d.drg_description,
    d.drg_category,
    d.drg_severity,
    
    -- Aggregated metrics
    COUNT(DISTINCT c.charge_key) AS total_charge_records,
    COUNT(DISTINCT c.hospital_key) AS hospital_count,
    SUM(c.total_discharges) AS total_discharges,
    SUM(c.total_covered_charges) AS total_covered_charges,
    SUM(c.total_payments) AS total_payments,
    SUM(c.total_medicare_payments) AS total_medicare_payments,
    
    -- Average metrics
    AVG(c.avg_covered_charges) AS avg_covered_charges,
    AVG(c.avg_total_payment) AS avg_total_payment,
    AVG(c.avg_medicare_payment) AS avg_medicare_payment,
    AVG(c.markup_ratio) AS avg_markup_ratio,
    
    -- Min/Max for ranges
    MIN(c.avg_covered_charges) AS min_charge,
    MAX(c.avg_covered_charges) AS max_charge,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY c.avg_covered_charges) AS median_charge,
    
    -- Data quality
    SUM(CASE WHEN c.has_data_quality_issues THEN 1 ELSE 0 END) AS records_with_quality_issues

FROM raw_marts.fct_inpatient_charges c

INNER JOIN raw_marts.dim_drg_codes d
    ON c.drg_key = d.drg_key

GROUP BY 
    d.drg_key,
    d.drg_code,
    d.drg_description,
    d.drg_category,
    d.drg_severity

ORDER BY total_covered_charges DESC

