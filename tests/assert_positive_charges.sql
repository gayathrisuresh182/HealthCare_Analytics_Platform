-- Custom test: Assert that all charges and payments are positive
-- Purpose: Data quality check for financial measures

SELECT 
    charge_key,
    hospital_key,
    drg_key,
    total_discharges,
    avg_covered_charges,
    avg_total_payment,
    avg_medicare_payment
FROM {{ ref('fct_inpatient_charges') }}
WHERE avg_covered_charges <= 0
   OR avg_total_payment <= 0
   OR avg_medicare_payment <= 0
   OR total_discharges <= 0

