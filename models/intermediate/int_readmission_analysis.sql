{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

-- Intermediate model: Readmission analysis with window functions
-- Demonstrates: Window functions for comparative analysis, aggregations
-- Purpose: Calculate hospital-level readmission performance metrics

WITH base_readmissions AS (
    SELECT * FROM {{ ref('stg_readmissions') }}
),

measure_categories AS (
    SELECT
        facility_id,
        facility_name,
        state,
        measure_name,
        number_of_discharges,
        number_of_readmissions,
        excess_readmission_ratio,
        predicted_readmission_rate,
        expected_readmission_rate,
        start_date,
        end_date,
        
        -- Categorize measure types
        CASE 
            WHEN measure_name LIKE '%AMI%' THEN 'Acute Myocardial Infarction'
            WHEN measure_name LIKE '%HF%' THEN 'Heart Failure'
            WHEN measure_name LIKE '%PN%' THEN 'Pneumonia'
            WHEN measure_name LIKE '%COPD%' THEN 'COPD'
            WHEN measure_name LIKE '%CABG%' THEN 'Coronary Artery Bypass Graft'
            WHEN measure_name LIKE '%THA%' OR measure_name LIKE '%TKA%' THEN 'Hip/Knee Replacement'
            ELSE 'Other'
        END AS measure_category
        
    FROM base_readmissions
    WHERE excess_readmission_ratio IS NOT NULL
      AND number_of_discharges > 0
),

hospital_aggregates AS (
    SELECT
        facility_id,
        facility_name,
        state,
        COUNT(DISTINCT measure_name) AS measure_count,
        SUM(number_of_discharges) AS total_discharges,
        SUM(number_of_readmissions) AS total_readmissions,
        AVG(excess_readmission_ratio) AS avg_excess_readmission_ratio,
        AVG(predicted_readmission_rate) AS avg_predicted_rate,
        AVG(expected_readmission_rate) AS avg_expected_rate,
        -- Weighted average by discharges
        SUM(excess_readmission_ratio * number_of_discharges) / NULLIF(SUM(number_of_discharges), 0) AS weighted_excess_ratio
    FROM measure_categories
    GROUP BY facility_id, facility_name, state
),

state_statistics AS (
    SELECT
        state,
        COUNT(*) AS hospital_count,
        AVG(avg_excess_readmission_ratio) AS state_avg_excess_ratio,
        STDDEV(avg_excess_readmission_ratio) AS state_stddev_excess_ratio,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_excess_readmission_ratio) AS state_median_excess_ratio
    FROM hospital_aggregates
    GROUP BY state
),

ranked_analysis AS (
    SELECT
        ha.facility_id,
        ha.facility_name,
        ha.state,
        ha.measure_count,
        ha.total_discharges,
        ha.total_readmissions,
        ha.avg_excess_readmission_ratio,
        ha.avg_predicted_rate,
        ha.avg_expected_rate,
        ha.weighted_excess_ratio,
        ss.hospital_count AS state_hospital_count,
        ss.state_avg_excess_ratio,
        ss.state_stddev_excess_ratio,
        ss.state_median_excess_ratio,
        
        -- Window functions for ranking
        RANK() OVER (ORDER BY ha.avg_excess_readmission_ratio ASC) AS rank_best_performance,
        DENSE_RANK() OVER (ORDER BY ha.avg_excess_readmission_ratio DESC) AS rank_worst_performance,
        ROW_NUMBER() OVER (PARTITION BY ha.state ORDER BY ha.avg_excess_readmission_ratio ASC) AS state_rank,
        
        -- Percentile analysis
        NTILE(5) OVER (ORDER BY ha.avg_excess_readmission_ratio) AS performance_quintile,
        NTILE(10) OVER (ORDER BY ha.avg_excess_readmission_ratio) AS performance_decile,
        
        -- Statistical comparisons
        AVG(ha.avg_excess_readmission_ratio) OVER (PARTITION BY ha.state) AS state_avg_window,
        STDDEV(ha.avg_excess_readmission_ratio) OVER (PARTITION BY ha.state) AS state_stddev_window,
        
        -- Z-score for performance
        CASE 
            WHEN STDDEV(ha.avg_excess_readmission_ratio) OVER (PARTITION BY ha.state) > 0
            THEN (ha.avg_excess_readmission_ratio - AVG(ha.avg_excess_readmission_ratio) OVER (PARTITION BY ha.state))
                 / STDDEV(ha.avg_excess_readmission_ratio) OVER (PARTITION BY ha.state)
            ELSE NULL
        END AS performance_z_score,
        
        -- Running totals
        SUM(ha.total_discharges) OVER (ORDER BY ha.avg_excess_readmission_ratio ASC) AS running_total_discharges,
        
        -- First and last values
        FIRST_VALUE(ha.avg_excess_readmission_ratio) OVER (PARTITION BY ha.state ORDER BY ha.avg_excess_readmission_ratio ASC) AS state_best_ratio,
        LAST_VALUE(ha.avg_excess_readmission_ratio) OVER (PARTITION BY ha.state ORDER BY ha.avg_excess_readmission_ratio ASC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state_worst_ratio
        
    FROM hospital_aggregates ha
    LEFT JOIN state_statistics ss
        ON ha.state = ss.state
)

SELECT * FROM ranked_analysis

