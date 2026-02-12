{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Dimension: Geography
-- Purpose: Master geography dimension with state, county, urban/rural classification
-- Source: stg_ipps_charges and stg_hospitals

WITH charges_geo AS (
    SELECT DISTINCT
        state_abbreviation,
        state_fips_code,
        city,
        zip_code,
        ruca_code,
        ruca_description
    FROM {{ ref('stg_ipps_charges') }}
    WHERE state_abbreviation IS NOT NULL
),

hospitals_geo AS (
    SELECT DISTINCT
        state,
        city,
        county,
        zip_code
    FROM {{ ref('stg_hospitals') }}
    WHERE state IS NOT NULL
),

combined_geo AS (
    SELECT
        COALESCE(c.state_abbreviation, h.state) AS state_abbreviation,
        c.state_fips_code,
        COALESCE(c.city, h.city) AS city,
        COALESCE(c.zip_code, h.zip_code) AS zip_code,
        h.county,
        c.ruca_code,
        c.ruca_description
    FROM charges_geo c
    FULL OUTER JOIN hospitals_geo h
        ON c.state_abbreviation = h.state
        AND c.city = h.city
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['state_abbreviation', 'city', 'zip_code']) }} AS geography_key,
        state_abbreviation,
        state_fips_code,
        city,
        zip_code,
        county,
        ruca_code,
        ruca_description,
        
        -- Urban/Rural classification based on RUCA codes
        CASE 
            WHEN ruca_code IN (1, 2, 3) THEN 'Urban'
            WHEN ruca_code IN (4, 5, 6) THEN 'Large Rural'
            WHEN ruca_code IN (7, 8, 9) THEN 'Small Rural'
            WHEN ruca_code = 10 THEN 'Remote Rural'
            ELSE 'Unknown'
        END AS urban_rural_classification,
        
        -- Census region mapping
        CASE 
            WHEN state_abbreviation IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA') THEN 'Northeast'
            WHEN state_abbreviation IN ('OH', 'IN', 'IL', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
            WHEN state_abbreviation IN ('DE', 'MD', 'DC', 'VA', 'WV', 'KY', 'TN', 'NC', 'SC', 'GA', 'FL', 'AL', 'MS', 'AR', 'LA', 'OK', 'TX') THEN 'South'
            WHEN state_abbreviation IN ('MT', 'ID', 'WY', 'CO', 'NM', 'AZ', 'UT', 'NV', 'WA', 'OR', 'CA', 'AK', 'HI') THEN 'West'
            ELSE 'Other'
        END AS census_region,
        
        -- Census division (more granular)
        CASE 
            WHEN state_abbreviation IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT') THEN 'New England'
            WHEN state_abbreviation IN ('NY', 'NJ', 'PA') THEN 'Middle Atlantic'
            WHEN state_abbreviation IN ('OH', 'IN', 'IL', 'MI', 'WI') THEN 'East North Central'
            WHEN state_abbreviation IN ('MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'West North Central'
            WHEN state_abbreviation IN ('DE', 'MD', 'DC', 'VA', 'WV', 'KY', 'TN') THEN 'East South Central'
            WHEN state_abbreviation IN ('NC', 'SC', 'GA', 'FL', 'AL', 'MS') THEN 'South Atlantic'
            WHEN state_abbreviation IN ('AR', 'LA', 'OK', 'TX') THEN 'West South Central'
            WHEN state_abbreviation IN ('MT', 'ID', 'WY', 'CO', 'NM', 'AZ', 'UT', 'NV') THEN 'Mountain'
            WHEN state_abbreviation IN ('WA', 'OR', 'CA', 'AK', 'HI') THEN 'Pacific'
            ELSE 'Other'
        END AS census_division
        
    FROM combined_geo
)

SELECT * FROM enriched

