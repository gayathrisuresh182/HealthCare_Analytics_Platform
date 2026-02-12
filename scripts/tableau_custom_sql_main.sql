-- Custom SQL for Tableau - Main Dashboard Dataset
-- This query joins all tables and filters to current hospitals
-- Use this in Tableau: Connect → Snowflake → New Custom SQL

SELECT 
    -- Fact table fields
    c.charge_key,
    c.hospital_key,
    c.drg_key,
    c.geography_key,
    c.hospital_id,
    c.drg_code,
    
    -- Measures
    c.total_discharges,
    c.avg_total_payment,
    c.avg_medicare_payment,
    c.avg_covered_charges,
    c.avg_covered_charges_original,
    c.avg_covered_charges_fixed,
    c.total_covered_charges,
    c.total_payments,
    c.total_medicare_payments,
    
    -- Business metrics
    c.markup_ratio,
    c.markup_ratio_original,
    c.medicare_payment_ratio,
    c.revenue_per_discharge,
    
    -- Data quality flags
    c.data_quality_flag_covered_charges_issue,
    c.data_quality_flag_capped_charges,
    c.data_quality_flag_capped_markup_ratio,
    c.has_orphaned_hospital,
    c.has_data_quality_issues,
    
    -- Hospital dimension fields
    h.facility_id,
    h.facility_name,
    h.address,
    h.city,
    h.state AS hospital_state,
    h.zip_code,
    h.county,
    h.telephone_number,
    h.hospital_type,
    h.hospital_ownership,
    h.emergency_services,
    h.birthing_friendly,
    h.hospital_overall_rating,
    h.rating_footnote,
    
    -- DRG dimension fields
    d.drg_code AS drg_code_dim,
    d.drg_description,
    d.drg_category_code,
    d.drg_category_description,
    
    -- Geography dimension fields
    g.state_abbreviation,
    g.county,
    g.city AS geography_city,
    g.zip_code AS geography_zip,
    g.urban_rural_classification,
    g.census_region,
    g.census_division,
    
    -- Calculated fields (for Tableau)
    CASE 
        WHEN c.total_discharges > 0 
        THEN c.total_covered_charges / c.total_discharges 
        ELSE NULL 
    END AS charge_per_discharge,
    
    CASE 
        WHEN c.has_data_quality_issues THEN 0 
        ELSE 100 
    END AS quality_score_row

FROM raw_marts.fct_inpatient_charges c

-- Join to hospitals (only current)
INNER JOIN raw_marts.dim_hospitals h
    ON c.hospital_key = h.hospital_key
    AND h.is_current = TRUE  -- Filter to current hospitals only

-- Join to DRG codes
INNER JOIN raw_marts.dim_drg_codes d
    ON c.drg_key = d.drg_key

-- Join to geography
INNER JOIN raw_marts.dim_geography g
    ON c.geography_key = g.geography_key

-- Optional: Add filters here if needed
-- WHERE c.hospital_key IS NOT NULL  -- Already handled by INNER JOIN
-- AND c.total_discharges > 0  -- Uncomment if you want to filter

