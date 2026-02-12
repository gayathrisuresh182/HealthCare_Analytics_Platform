{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Fact: Hospital Summary (Aggregated from detail facts)
-- Grain: One row per hospital
-- Source: Aggregated FROM fct_inpatient_charges + fct_readmissions
-- Purpose: Hospital-level summary metrics for dashboards

WITH charges_detail AS (
    SELECT * FROM {{ ref('fct_inpatient_charges') }}
),

readmissions_detail AS (
    SELECT * FROM {{ ref('fct_readmissions') }}
),

hospitals AS (
    SELECT hospital_key, facility_id FROM {{ ref('dim_hospitals') }}
    WHERE is_current = TRUE
),

charges_aggregated AS (
    SELECT
        hospital_key,
        COUNT(DISTINCT drg_code) AS distinct_drg_count,
        SUM(total_discharges) AS total_discharges,
        SUM(total_covered_charges) AS total_covered_charges,
        SUM(total_payments) AS total_payments,
        SUM(total_medicare_payments) AS total_medicare_payments,
        AVG(avg_covered_charges) AS avg_covered_charges,
        AVG(avg_total_payment) AS avg_total_payment,
        AVG(avg_medicare_payment) AS avg_medicare_payment,
        AVG(markup_ratio) AS avg_markup_ratio,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_covered_charges) AS median_charge,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_total_payment) AS median_payment
    FROM charges_detail
    GROUP BY hospital_key
),

readmissions_aggregated AS (
    SELECT
        hospital_key,
        COUNT(DISTINCT measure_name) AS readmission_measure_count,
        SUM(number_of_discharges) AS total_readmission_discharges,
        SUM(number_of_readmissions) AS total_readmissions,
        AVG(excess_readmission_ratio) AS avg_excess_readmission_ratio,
        AVG(predicted_readmission_rate) AS avg_predicted_rate,
        AVG(expected_readmission_rate) AS avg_expected_rate,
        -- Weighted average by discharges
        SUM(excess_readmission_ratio * number_of_discharges) / NULLIF(SUM(number_of_discharges), 0) AS weighted_excess_ratio
    FROM readmissions_detail
    GROUP BY hospital_key
)

SELECT
    h.hospital_key,
    h.facility_id,
    
    -- Charge metrics
    COALESCE(ca.distinct_drg_count, 0) AS distinct_drg_count,
    COALESCE(ca.total_discharges, 0) AS total_discharges,
    COALESCE(ca.total_covered_charges, 0) AS total_covered_charges,
    COALESCE(ca.total_payments, 0) AS total_payments,
    COALESCE(ca.total_medicare_payments, 0) AS total_medicare_payments,
    ca.avg_covered_charges,
    ca.avg_total_payment,
    ca.avg_medicare_payment,
    ca.avg_markup_ratio,
    ca.median_charge,
    ca.median_payment,
    
    -- Readmission metrics
    COALESCE(ra.readmission_measure_count, 0) AS readmission_measure_count,
    COALESCE(ra.total_readmission_discharges, 0) AS total_readmission_discharges,
    COALESCE(ra.total_readmissions, 0) AS total_readmissions,
    ra.avg_excess_readmission_ratio,
    ra.avg_predicted_rate,
    ra.avg_expected_rate,
    ra.weighted_excess_ratio,
    
    -- Combined metrics
    CASE 
        WHEN ca.total_discharges > 0 AND ra.total_readmission_discharges > 0
        THEN ra.total_readmissions / ca.total_discharges
        ELSE NULL
    END AS overall_readmission_rate,
    
    CASE 
        WHEN ca.total_discharges > 0
        THEN ca.total_payments / ca.total_discharges
        ELSE NULL
    END AS revenue_per_discharge

FROM hospitals h
LEFT JOIN charges_aggregated ca
    ON h.hospital_key = ca.hospital_key
LEFT JOIN readmissions_aggregated ra
    ON h.hospital_key = ra.hospital_key

