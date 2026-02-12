-- Custom test: Assert business rules
-- Purpose: Validate business logic constraints
-- Note: Tests check data quality flags to identify source data issues, not fixed values
-- This test is expected to find data quality issues in source data
-- Configured as warning to not fail CI/CD pipeline

{{ config(severity='warn') }}

-- Rule 1: Covered charges should be >= Medicare payments (check original values)
SELECT 
    charge_key,
    hospital_key,
    avg_covered_charges_original AS avg_covered_charges,
    avg_medicare_payment,
    'Covered charges less than Medicare payment' AS rule_violation
FROM {{ ref('fct_inpatient_charges') }}
WHERE data_quality_flag_covered_charges_issue = TRUE

UNION ALL

-- Rule 2: Total payments should be >= Medicare payments
SELECT 
    charge_key,
    hospital_key,
    avg_total_payment,
    avg_medicare_payment,
    'Total payment less than Medicare payment' AS rule_violation
FROM {{ ref('fct_inpatient_charges') }}
WHERE avg_total_payment < avg_medicare_payment

UNION ALL

-- Rule 3: Observed readmission rate should be reasonable
SELECT 
    readmission_key,
    hospital_key,
    number_of_readmissions,
    number_of_discharges,
    'Readmissions exceed discharges' AS rule_violation
FROM {{ ref('fct_readmissions') }}
WHERE number_of_readmissions > number_of_discharges
