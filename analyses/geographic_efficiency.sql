-- Analysis: Geographic Efficiency Analysis
-- Purpose: Compare cost and quality metrics across geographic regions
-- Demonstrates: Window functions, geographic aggregations

WITH state_metrics AS (
    SELECT
        ss.state_abbreviation,
        ss.census_region,
        ss.census_division,
        ss.hospital_count,
        ss.state_avg_total_payment,
        ss.state_avg_excess_readmission_ratio,
        ss.state_median_payment,
        ss.state_revenue_per_discharge
    FROM {{ ref('fct_state_summary') }} ss
),

ranked_states AS (
    SELECT
        *,
        -- Rankings
        RANK() OVER (ORDER BY state_avg_total_payment ASC) AS rank_lowest_cost,
        RANK() OVER (ORDER BY state_avg_excess_readmission_ratio ASC) AS rank_best_quality,
        DENSE_RANK() OVER (PARTITION BY census_region ORDER BY state_avg_total_payment ASC) AS region_cost_rank,
        
        -- Percentiles
        NTILE(4) OVER (ORDER BY state_avg_total_payment) AS cost_quartile,
        NTILE(10) OVER (ORDER BY state_avg_excess_readmission_ratio) AS quality_decile,
        
        -- Regional comparisons
        AVG(state_avg_total_payment) OVER (PARTITION BY census_region) AS region_avg_cost,
        STDDEV(state_avg_total_payment) OVER (PARTITION BY census_region) AS region_stddev_cost,
        
        -- Z-scores
        CASE 
            WHEN STDDEV(state_avg_total_payment) OVER (PARTITION BY census_region) > 0
            THEN (state_avg_total_payment - AVG(state_avg_total_payment) OVER (PARTITION BY census_region))
                 / STDDEV(state_avg_total_payment) OVER (PARTITION BY census_region)
            ELSE NULL
        END AS cost_z_score
        
    FROM state_metrics
)

SELECT
    state_abbreviation,
    census_region,
    census_division,
    hospital_count,
    state_avg_total_payment,
    state_median_payment,
    state_avg_excess_readmission_ratio,
    state_revenue_per_discharge,
    rank_lowest_cost,
    rank_best_quality,
    region_cost_rank,
    cost_quartile,
    quality_decile,
    region_avg_cost,
    region_stddev_cost,
    cost_z_score,
    
    -- Efficiency score (lower cost + better quality = higher score)
    CASE 
        WHEN cost_z_score IS NOT NULL AND state_avg_excess_readmission_ratio IS NOT NULL
        THEN (1.0 - ABS(cost_z_score)) * (2.0 - state_avg_excess_readmission_ratio)
        ELSE NULL
    END AS efficiency_score

FROM ranked_states
ORDER BY efficiency_score DESC NULLS LAST

