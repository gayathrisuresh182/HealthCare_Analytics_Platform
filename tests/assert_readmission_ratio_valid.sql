-- Custom test: Assert that readmission ratios are within reasonable bounds
-- Purpose: Data quality check for readmission metrics
-- Note: Rates are stored as percentages (0-100), not decimals (0-1)

SELECT DISTINCT
    readmission_key,
    hospital_key,
    measure_name,
    excess_readmission_ratio,
    predicted_readmission_rate,
    expected_readmission_rate
FROM {{ ref('fct_readmissions') }}
WHERE (excess_readmission_ratio IS NOT NULL AND (excess_readmission_ratio < 0 OR excess_readmission_ratio > 5.0))
   OR (predicted_readmission_rate IS NOT NULL AND (predicted_readmission_rate < 0 OR predicted_readmission_rate > 100.0))
   OR (expected_readmission_rate IS NOT NULL AND (expected_readmission_rate < 0 OR expected_readmission_rate > 100.0))

