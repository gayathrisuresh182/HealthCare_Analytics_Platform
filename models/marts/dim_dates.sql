{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Dimension: Date dimension table
-- Purpose: Date spine for time-based analysis
-- Date range: 2020-01-01 to 2023-12-31

WITH date_spine AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2020-01-01') AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 1461))  -- 4 years = ~1461 days
    WHERE date_day <= '2023-12-31'
),

enriched AS (
    SELECT
        date_day AS date_key,
        date_day,
        
        -- Date components
        YEAR(date_day) AS year,
        MONTH(date_day) AS month,
        DAY(date_day) AS day,
        DAYOFWEEK(date_day) AS day_of_week,
        DAYOFYEAR(date_day) AS day_of_year,
        WEEK(date_day) AS week_of_year,
        QUARTER(date_day) AS quarter,
        
        -- Date names
        DAYNAME(date_day) AS day_name,
        MONTHNAME(date_day) AS month_name,
        
        -- Fiscal year (assuming Oct 1 start)
        CASE 
            WHEN MONTH(date_day) >= 10 THEN YEAR(date_day) + 1
            ELSE YEAR(date_day)
        END AS fiscal_year,
        
        CASE 
            WHEN MONTH(date_day) >= 10 THEN MONTH(date_day) - 9
            ELSE MONTH(date_day) + 3
        END AS fiscal_quarter,
        
        -- Flags
        CASE WHEN DAYOFWEEK(date_day) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
        CASE WHEN DAYOFWEEK(date_day) NOT IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekday,
        
        -- First/last of period flags
        CASE WHEN DAY(date_day) = 1 THEN TRUE ELSE FALSE END AS is_first_of_month,
        CASE WHEN date_day = LAST_DAY(date_day) THEN TRUE ELSE FALSE END AS is_last_of_month,
        CASE WHEN DAY(date_day) = 1 AND MONTH(date_day) = 1 THEN TRUE ELSE FALSE END AS is_first_of_year,
        CASE WHEN date_day = LAST_DAY(date_day) AND MONTH(date_day) = 12 THEN TRUE ELSE FALSE END AS is_last_of_year
        
    FROM date_spine
)

SELECT * FROM enriched

