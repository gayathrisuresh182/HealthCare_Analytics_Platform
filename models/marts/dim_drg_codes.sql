{{
    config(
        materialized='table',
        schema='marts'
    )
}}

-- Dimension: DRG Codes
-- Purpose: Master list of DRG codes with descriptions
-- Source: stg_ipps_charges (extract unique DRG codes)

WITH unique_drgs AS (
    SELECT DISTINCT
        drg_code,
        drg_description
    FROM {{ ref('stg_ipps_charges') }}
    WHERE drg_code IS NOT NULL
),

enriched AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['drg_code']) }} AS drg_key,
        drg_code,
        drg_description,
        
        -- Extract DRG category from code (first digit or first 2 digits)
        LEFT(drg_code, 1) AS drg_category_code,
        CASE 
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 1 AND 9 THEN 'Pre-Major Diagnostic Categories'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 10 AND 39 THEN 'Nervous System'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 40 AND 57 THEN 'Eye'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 57 AND 103 THEN 'Ear, Nose, Mouth & Throat'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 104 AND 115 THEN 'Respiratory System'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 116 AND 146 THEN 'Circulatory System'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 147 AND 159 THEN 'Digestive System'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 157 AND 181 THEN 'Hepatobiliary System & Pancreas'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 182 AND 195 THEN 'Musculoskeletal System & Connective Tissue'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 196 AND 199 THEN 'Skin, Subcutaneous Tissue & Breast'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 200 AND 207 THEN 'Endocrine, Nutritional & Metabolic'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 208 AND 214 THEN 'Kidney & Urinary Tract'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 215 AND 224 THEN 'Male Reproductive System'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 225 AND 227 THEN 'Female Reproductive System'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 228 AND 235 THEN 'Pregnancy, Childbirth & Puerperium'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 236 AND 244 THEN 'Newborns & Other Neonates'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 245 THEN 'Mental Diseases & Disorders'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 246 AND 251 THEN 'Alcohol/Drug Use & Induced Mental Disorders'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 252 AND 256 THEN 'Injuries, Poisonings & Toxic Effects'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 257 AND 259 THEN 'Burns'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 260 AND 264 THEN 'Factors Influencing Health Status'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 265 THEN 'Multiple Significant Trauma'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) BETWEEN 266 AND 267 THEN 'Human Immunodeficiency Virus Infections'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 268 THEN 'Transplants'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 269 THEN 'Extensive Procedures'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 470 THEN 'Ungroupable'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 981 THEN 'Extensive O.R. Procedure Unrelated to Principal Diagnosis'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 982 THEN 'Non-Extensive O.R. Procedure Unrelated to Principal Diagnosis'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 983 THEN 'Non-O.R. Procedure Unrelated to Principal Diagnosis'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 984 THEN 'Unrelated Operating Room Procedure'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 985 THEN 'Non-Extensive O.R. Procedure'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 986 THEN 'Non-O.R. Procedure'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 987 THEN 'Non-O.R. Procedure'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 988 THEN 'Non-O.R. Procedure'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 989 THEN 'Non-O.R. Procedure'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 998 THEN 'Principal Diagnosis Invalid as Discharge'
            WHEN CAST(LEFT(drg_code, 2) AS INTEGER) = 999 THEN 'Ungroupable'
            ELSE 'Other'
        END AS drg_category_description
        
    FROM unique_drgs
)

SELECT * FROM enriched

