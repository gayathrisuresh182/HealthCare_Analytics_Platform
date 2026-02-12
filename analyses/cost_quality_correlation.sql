-- Analysis: Cost-Quality Correlation
-- Purpose: Analyze correlation between hospital costs and quality ratings
-- Demonstrates: Statistical functions, correlation analysis

WITH hospital_metrics AS (
    SELECT
        h.hospital_key,
        h.hospital_overall_rating,
        hs.total_payments,
        hs.total_discharges,
        hs.avg_total_payment,
        hs.avg_excess_readmission_ratio,
        CASE 
            WHEN h.hospital_overall_rating IS NOT NULL THEN h.hospital_overall_rating
            ELSE NULL
        END AS rating
    FROM {{ ref('fct_hospital_summary') }} hs
    INNER JOIN {{ ref('dim_hospitals') }} h
        ON hs.hospital_key = h.hospital_key
    WHERE h.is_current = TRUE
      AND h.hospital_overall_rating IS NOT NULL
      AND hs.total_payments > 0
)

SELECT
    -- Correlation analysis
    CORR(rating, avg_total_payment) AS cost_rating_correlation,
    CORR(rating, avg_excess_readmission_ratio) AS quality_readmission_correlation,
    CORR(avg_total_payment, avg_excess_readmission_ratio) AS cost_readmission_correlation,
    
    -- Rating-based analysis
    rating,
    COUNT(*) AS hospital_count,
    AVG(avg_total_payment) AS avg_cost_by_rating,
    STDDEV(avg_total_payment) AS stddev_cost_by_rating,
    AVG(avg_excess_readmission_ratio) AS avg_readmission_by_rating,
    
    -- Statistical tests
    VARIANCE(avg_total_payment) AS cost_variance,
    VARIANCE(avg_excess_readmission_ratio) AS readmission_variance

FROM hospital_metrics
GROUP BY rating
ORDER BY rating

