{{
    config(
        materialized='view',
        schema='intermediate'
    )
}}

-- Intermediate model: Advanced cost and efficiency metrics using window functions
-- Demonstrates: Multiple window functions, CTEs, statistical calculations
-- Purpose: Calculate hospital-level cost efficiency rankings and percentiles

WITH base_charges AS (
    SELECT * FROM {{ ref('stg_ipps_charges') }}
),

hospital_totals AS (
    SELECT
        hospital_id,
        hospital_name,
        state_abbreviation,
        SUM(total_discharges) AS total_hospital_discharges,
        SUM(avg_covered_charges * total_discharges) AS total_covered_charges,
        SUM(avg_total_payment * total_discharges) AS total_payments,
        SUM(avg_medicare_payment * total_discharges) AS total_medicare_payments,
        COUNT(DISTINCT drg_code) AS distinct_drg_count,
        AVG(avg_covered_charges) AS avg_charge_per_drg,
        AVG(avg_total_payment) AS avg_payment_per_drg,
        AVG(avg_medicare_payment) AS avg_medicare_payment_per_drg
    FROM base_charges
    WHERE total_discharges > 0
      AND avg_covered_charges > 0
      AND avg_total_payment > 0
    GROUP BY hospital_id, hospital_name, state_abbreviation
),

calculated_metrics AS (
    SELECT
        hospital_id,
        hospital_name,
        state_abbreviation,
        total_hospital_discharges,
        total_covered_charges,
        total_payments,
        total_medicare_payments,
        distinct_drg_count,
        avg_charge_per_drg,
        avg_payment_per_drg,
        avg_medicare_payment_per_drg,
        
        -- Calculate markup ratio (covered charges / Medicare payment)
        CASE 
            WHEN total_medicare_payments > 0 
            THEN total_covered_charges / total_medicare_payments 
            ELSE NULL 
        END AS markup_ratio,
        
        -- Calculate payment efficiency (Medicare payment / total payment)
        CASE 
            WHEN total_payments > 0 
            THEN total_medicare_payments / total_payments 
            ELSE NULL 
        END AS medicare_payment_ratio
        
    FROM hospital_totals
),

state_statistics AS (
    SELECT
        state_abbreviation,
        COUNT(*) AS hospital_count,
        AVG(total_hospital_discharges) AS avg_state_discharges,
        STDDEV(total_hospital_discharges) AS stddev_state_discharges,
        AVG(avg_charge_per_drg) AS avg_state_charge_per_drg,
        AVG(avg_payment_per_drg) AS avg_state_payment_per_drg,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_charge_per_drg) AS median_state_charge,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_payment_per_drg) AS median_state_payment
    FROM calculated_metrics
    GROUP BY state_abbreviation
),

ranked_metrics AS (
    SELECT
        cm.hospital_id,
        cm.hospital_name,
        cm.state_abbreviation,
        cm.total_hospital_discharges,
        cm.total_covered_charges,
        cm.total_payments,
        cm.total_medicare_payments,
        cm.distinct_drg_count,
        cm.avg_charge_per_drg,
        cm.avg_payment_per_drg,
        cm.avg_medicare_payment_per_drg,
        cm.markup_ratio,
        cm.medicare_payment_ratio,
        ss.hospital_count AS state_hospital_count,
        ss.avg_state_discharges,
        ss.stddev_state_discharges,
        ss.avg_state_charge_per_drg,
        ss.avg_state_payment_per_drg,
        ss.median_state_charge,
        ss.median_state_payment,
        
        -- Window functions for rankings
        RANK() OVER (ORDER BY cm.total_hospital_discharges DESC) AS rank_by_volume,
        DENSE_RANK() OVER (ORDER BY cm.avg_charge_per_drg DESC) AS rank_by_avg_charge,
        ROW_NUMBER() OVER (PARTITION BY cm.state_abbreviation ORDER BY cm.total_hospital_discharges DESC) AS state_volume_rank,
        
        -- Percentile rankings using NTILE
        NTILE(10) OVER (ORDER BY cm.avg_charge_per_drg) AS charge_percentile_decile,
        NTILE(4) OVER (ORDER BY cm.avg_payment_per_drg) AS payment_percentile_quartile,
        
        -- Statistical comparisons
        AVG(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation) AS state_avg_charge_window,
        STDDEV(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation) AS state_stddev_charge_window,
        
        -- Relative performance (z-score approximation)
        CASE 
            WHEN STDDEV(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation) > 0
            THEN (cm.avg_charge_per_drg - AVG(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation)) 
                 / STDDEV(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation)
            ELSE NULL
        END AS charge_z_score,
        
        -- Running totals
        SUM(cm.total_hospital_discharges) OVER (ORDER BY cm.total_hospital_discharges DESC) AS running_total_discharges,
        SUM(cm.total_hospital_discharges) OVER (PARTITION BY cm.state_abbreviation ORDER BY cm.total_hospital_discharges DESC) AS state_running_total_discharges,
        
        -- First and last values
        FIRST_VALUE(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation ORDER BY cm.total_hospital_discharges DESC) AS state_top_charge,
        LAST_VALUE(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation ORDER BY cm.total_hospital_discharges DESC 
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS state_bottom_charge,
        
        -- Lag and lead for trend analysis
        LAG(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation ORDER BY cm.total_hospital_discharges DESC) AS prev_hospital_charge,
        LEAD(cm.avg_charge_per_drg) OVER (PARTITION BY cm.state_abbreviation ORDER BY cm.total_hospital_discharges DESC) AS next_hospital_charge
        
    FROM calculated_metrics cm
    LEFT JOIN state_statistics ss
        ON cm.state_abbreviation = ss.state_abbreviation
)

SELECT * FROM ranked_metrics

