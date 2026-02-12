{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Fact: State Summary (Aggregated from hospital summary)
-- Grain: One row per state
-- Source: Aggregated FROM fct_hospital_summary
-- Purpose: State-level summary metrics for regional analysis

WITH hospital_summary AS (
    SELECT * FROM {{ ref('fct_hospital_summary') }}
),

geography AS (
    SELECT DISTINCT 
        geography_key,
        state_abbreviation,
        census_region,
        census_division
    FROM {{ ref('dim_geography') }}
),

hospital_geography AS (
    SELECT 
        hs.*,
        g.state_abbreviation,
        g.census_region,
        g.census_division
    FROM hospital_summary hs
    INNER JOIN {{ ref('dim_hospitals') }} h
        ON hs.hospital_key = h.hospital_key
    INNER JOIN geography g
        ON h.state = g.state_abbreviation
    WHERE h.is_current = TRUE
),

state_aggregated AS (
    SELECT
        state_abbreviation,
        MAX(census_region) AS census_region,
        MAX(census_division) AS census_division,
        COUNT(DISTINCT hospital_key) AS hospital_count,
        
        -- Charge metrics (sums and averages)
        SUM(total_discharges) AS state_total_discharges,
        SUM(total_covered_charges) AS state_total_covered_charges,
        SUM(total_payments) AS state_total_payments,
        SUM(total_medicare_payments) AS state_total_medicare_payments,
        AVG(avg_covered_charges) AS state_avg_covered_charges,
        AVG(avg_total_payment) AS state_avg_total_payment,
        AVG(avg_medicare_payment) AS state_avg_medicare_payment,
        AVG(avg_markup_ratio) AS state_avg_markup_ratio,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_covered_charges) AS state_median_charge,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_total_payment) AS state_median_payment,
        
        -- Readmission metrics
        AVG(avg_excess_readmission_ratio) AS state_avg_excess_readmission_ratio,
        AVG(weighted_excess_ratio) AS state_weighted_excess_ratio,
        SUM(total_readmissions) AS state_total_readmissions,
        SUM(total_readmission_discharges) AS state_total_readmission_discharges,
        
        -- Statistical measures
        STDDEV(avg_covered_charges) AS state_stddev_charges,
        STDDEV(avg_total_payment) AS state_stddev_payments,
        STDDEV(avg_excess_readmission_ratio) AS state_stddev_readmission_ratio,
        
        -- Min/Max for range analysis
        MIN(avg_covered_charges) AS state_min_charge,
        MAX(avg_covered_charges) AS state_max_charge,
        MIN(avg_excess_readmission_ratio) AS state_best_readmission_ratio,
        MAX(avg_excess_readmission_ratio) AS state_worst_readmission_ratio
        
    FROM hospital_geography
    GROUP BY state_abbreviation
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['state_abbreviation']) }} AS state_summary_key,
    state_abbreviation,
    census_region,
    census_division,
    hospital_count,
    
    -- Charge metrics
    state_total_discharges,
    state_total_covered_charges,
    state_total_payments,
    state_total_medicare_payments,
    state_avg_covered_charges,
    state_avg_total_payment,
    state_avg_medicare_payment,
    state_avg_markup_ratio,
    state_median_charge,
    state_median_payment,
    state_stddev_charges,
    state_stddev_payments,
    state_min_charge,
    state_max_charge,
    
    -- Readmission metrics
    state_avg_excess_readmission_ratio,
    state_weighted_excess_ratio,
    state_total_readmissions,
    state_total_readmission_discharges,
    state_stddev_readmission_ratio,
    state_best_readmission_ratio,
    state_worst_readmission_ratio,
    
    -- Calculated metrics
    CASE 
        WHEN state_total_discharges > 0
        THEN state_total_payments / state_total_discharges
        ELSE NULL
    END AS state_revenue_per_discharge,
    
    CASE 
        WHEN state_total_readmission_discharges > 0
        THEN state_total_readmissions / state_total_readmission_discharges
        ELSE NULL
    END AS state_overall_readmission_rate

FROM state_aggregated

