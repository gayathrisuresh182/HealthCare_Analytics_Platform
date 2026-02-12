-- Analysis: Ownership Impact on Cost and Quality
-- Purpose: Analyze how hospital ownership affects cost and quality metrics
-- Demonstrates: Grouping, statistical aggregations, comparative analysis

WITH ownership_metrics AS (
    SELECT
        h.hospital_ownership,
        hs.total_discharges,
        hs.total_payments,
        hs.avg_total_payment,
        hs.avg_excess_readmission_ratio,
        hs.weighted_excess_ratio,
        h.hospital_overall_rating
    FROM {{ ref('fct_hospital_summary') }} hs
    INNER JOIN {{ ref('dim_hospitals') }} h
        ON hs.hospital_key = h.hospital_key
    WHERE h.is_current = TRUE
      AND h.hospital_ownership IS NOT NULL
),

ownership_aggregated AS (
    SELECT
        hospital_ownership,
        COUNT(*) AS hospital_count,
        SUM(total_discharges) AS total_discharges,
        SUM(total_payments) AS total_payments,
        
        -- Cost metrics
        AVG(avg_total_payment) AS avg_cost,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_total_payment) AS median_cost,
        STDDEV(avg_total_payment) AS stddev_cost,
        MIN(avg_total_payment) AS min_cost,
        MAX(avg_total_payment) AS max_cost,
        
        -- Quality metrics
        AVG(avg_excess_readmission_ratio) AS avg_readmission_ratio,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY avg_excess_readmission_ratio) AS median_readmission_ratio,
        STDDEV(avg_excess_readmission_ratio) AS stddev_readmission_ratio,
        
        -- Rating metrics
        AVG(hospital_overall_rating) AS avg_rating,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY hospital_overall_rating) AS median_rating,
        COUNT(CASE WHEN hospital_overall_rating >= 4 THEN 1 END) AS high_rated_count,
        COUNT(CASE WHEN hospital_overall_rating <= 2 THEN 1 END) AS low_rated_count
        
    FROM ownership_metrics
    GROUP BY hospital_ownership
),

ranked_ownership AS (
    SELECT
        *,
        -- Rankings
        RANK() OVER (ORDER BY avg_cost ASC) AS rank_lowest_cost,
        RANK() OVER (ORDER BY avg_readmission_ratio ASC) AS rank_best_quality,
        RANK() OVER (ORDER BY avg_rating DESC) AS rank_highest_rating,
        
        -- Percentiles
        NTILE(4) OVER (ORDER BY avg_cost) AS cost_quartile,
        
        -- Comparative metrics
        AVG(avg_cost) OVER () AS overall_avg_cost,
        AVG(avg_readmission_ratio) OVER () AS overall_avg_readmission_ratio,
        AVG(avg_rating) OVER () AS overall_avg_rating,
        
        -- Differences from overall average
        avg_cost - AVG(avg_cost) OVER () AS cost_difference_from_avg,
        avg_readmission_ratio - AVG(avg_readmission_ratio) OVER () AS readmission_difference_from_avg
        
    FROM ownership_aggregated
)

SELECT
    hospital_ownership,
    hospital_count,
    total_discharges,
    total_payments,
    avg_cost,
    median_cost,
    stddev_cost,
    min_cost,
    max_cost,
    avg_readmission_ratio,
    median_readmission_ratio,
    stddev_readmission_ratio,
    avg_rating,
    median_rating,
    high_rated_count,
    low_rated_count,
    rank_lowest_cost,
    rank_best_quality,
    rank_highest_rating,
    cost_quartile,
    overall_avg_cost,
    overall_avg_readmission_ratio,
    overall_avg_rating,
    cost_difference_from_avg,
    readmission_difference_from_avg,
    
    -- Performance score
    CASE 
        WHEN overall_avg_cost > 0 AND overall_avg_readmission_ratio > 0
        THEN ((overall_avg_cost / avg_cost) * 0.5) + ((overall_avg_readmission_ratio / avg_readmission_ratio) * 0.5)
        ELSE NULL
    END AS performance_score

FROM ranked_ownership
ORDER BY performance_score DESC NULLS LAST

