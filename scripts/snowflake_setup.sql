-- ============================================================================
-- Phase 1B: Snowflake Setup Script
-- Healthcare Analytics Platform - Complete Database Setup
-- ============================================================================
-- 
-- This script creates:
-- 1. Database: HEALTHCARE_ANALYTICS
-- 2. 5 Schemas: raw, staging, intermediate, marts, snapshots
-- 3. 2 Virtual Warehouses: loading_wh, transforming_wh
-- 4. External Stage for S3
-- 5. 3 Raw Tables (matching CSV structure exactly)
-- 6. COPY INTO commands to load data from S3
--
-- Prerequisites:
-- - Snowflake account with ACCOUNTADMIN or SYSADMIN role
-- - S3 bucket: healthcare-analytics-datalake-gayat-2026
-- - AWS IAM role configured for Snowflake access
-- ============================================================================

-- ============================================================================
-- STEP 1: Create Database
-- ============================================================================

USE ROLE ACCOUNTADMIN;  -- Or SYSADMIN if you don't have ACCOUNTADMIN

CREATE OR REPLACE DATABASE HEALTHCARE_ANALYTICS
    COMMENT = 'Healthcare Analytics Platform - Main database for CMS data';

USE DATABASE HEALTHCARE_ANALYTICS;

-- ============================================================================
-- STEP 2: Create Schemas (Bronze, Silver, Gold layers)
-- ============================================================================

CREATE OR REPLACE SCHEMA raw
    COMMENT = 'Bronze layer - Raw data from S3, exact CSV structure';

CREATE OR REPLACE SCHEMA staging
    COMMENT = 'Silver layer - Cleaned and standardized data from dbt staging models';

CREATE OR REPLACE SCHEMA intermediate
    COMMENT = 'Silver layer - Business logic and complex transformations from dbt intermediate models';

CREATE OR REPLACE SCHEMA marts
    COMMENT = 'Gold layer - Final dimensional model (facts and dimensions) from dbt marts';

CREATE OR REPLACE SCHEMA snapshots
    COMMENT = 'SCD Type 2 tracking - Historical dimension changes from dbt snapshots';

-- ============================================================================
-- STEP 3: Create Virtual Warehouses
-- ============================================================================

-- Warehouse for loading data (small, auto-suspend)
CREATE OR REPLACE WAREHOUSE loading_wh
    WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60  -- Suspend after 60 seconds of inactivity
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for loading data from S3';

-- Warehouse for transformations (small, auto-suspend)
CREATE OR REPLACE WAREHOUSE transforming_wh
    WITH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for dbt transformations and queries';

-- Grant usage to your role
GRANT USAGE ON WAREHOUSE loading_wh TO ROLE SYSADMIN;
GRANT USAGE ON WAREHOUSE transforming_wh TO ROLE SYSADMIN;

-- ============================================================================
-- STEP 4: Create External Stage for S3
-- ============================================================================
-- 
-- NOTE: You'll need to configure AWS IAM role for Snowflake first.
-- See: docs/snowflake_s3_integration.md for detailed instructions
--
-- For now, using AWS_KEY_ID and AWS_SECRET_KEY (less secure but works for trial)
-- For production, use IAM role instead.
-- ============================================================================

USE SCHEMA raw;

-- Create file format for CSV
CREATE OR REPLACE FILE FORMAT csv_format
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    RECORD_DELIMITER = '\n'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    ESCAPE = 'NONE'
    ESCAPE_UNENCLOSED_FIELD = '\134'
    DATE_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO'
    NULL_IF = ('NULL', 'null', '')
    COMMENT = 'CSV file format for CMS data files';

-- Create external stage pointing to S3 bronze layer
-- REPLACE THESE VALUES:
--   - YOUR_AWS_KEY_ID: Your AWS Access Key ID
--   - YOUR_AWS_SECRET_KEY: Your AWS Secret Access Key
--   - YOUR_BUCKET_NAME: healthcare-analytics-datalake-gayat-2026

CREATE OR REPLACE STAGE healthcare_bronze_stage
    URL = 's3://your-bucket-name/bronze/raw/cms-data/'
    CREDENTIALS = (
        AWS_KEY_ID = 'YOUR_AWS_ACCESS_KEY_ID'
        AWS_SECRET_KEY = 'YOUR_AWS_SECRET_ACCESS_KEY'
    )
    FILE_FORMAT = csv_format
    COMMENT = 'External stage for S3 bronze layer - CMS healthcare data';


-- Test stage (list files)
-- LIST @healthcare_bronze_stage;

-- ============================================================================
-- STEP 5: Create Raw Tables (Exact CSV Structure)
-- ============================================================================

USE SCHEMA raw;

-- ----------------------------------------------------------------------------
-- Table 1: IPPS Charges (Dataset 1)
-- Source: ipps_charges.csv
-- Grain: One row per hospital per DRG code
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE raw.ipps_charges (
    Rndrng_Prvdr_CCN VARCHAR(50),
    Rndrng_Prvdr_Org_Name VARCHAR(500),
    Rndrng_Prvdr_City VARCHAR(200),
    Rndrng_Prvdr_St VARCHAR(500),
    Rndrng_Prvdr_State_FIPS VARCHAR(10),
    Rndrng_Prvdr_Zip5 VARCHAR(10),
    Rndrng_Prvdr_State_Abrvtn VARCHAR(10),
    Rndrng_Prvdr_RUCA VARCHAR(10),
    Rndrng_Prvdr_RUCA_Desc VARCHAR(200),
    DRG_Cd VARCHAR(10),
    DRG_Desc VARCHAR(500),
    Tot_Dschrgs VARCHAR(50),
    Avg_Submtd_Cvrd_Chrg VARCHAR(50),  -- Has $ and commas, will clean in dbt
    Avg_Tot_Pymt_Amt VARCHAR(50),      -- Has $ and commas, will clean in dbt
    Avg_Mdcr_Pymt_Amt VARCHAR(50)      -- Has $ and commas, will clean in dbt
)
COMMENT = 'Raw IPPS charges data - Medicare Inpatient Hospitals by Provider and Service. Exact CSV structure.';

-- ----------------------------------------------------------------------------
-- Table 2: Hospital General Info (Dataset 2)
-- Source: hospital_general_info.csv
-- Grain: One row per hospital
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE raw.hospital_general_info (
    "Facility ID" VARCHAR(50),
    "Facility Name" VARCHAR(500),
    "Address" VARCHAR(500),
    "City/Town" VARCHAR(200),
    "State" VARCHAR(10),
    "ZIP Code" VARCHAR(10),
    "County/Parish" VARCHAR(200),
    "Telephone Number" VARCHAR(50),
    "Hospital Type" VARCHAR(200),
    "Hospital Ownership" VARCHAR(200),
    "Emergency Services" VARCHAR(10),
    "Meets criteria for birthing friendly designation" VARCHAR(10),
    "Hospital overall rating" VARCHAR(10),
    "Hospital overall rating footnote" VARCHAR(1000),
    "MORT Group Measure Count" VARCHAR(10),
    "Count of Facility MORT Measures" VARCHAR(10),
    "Count of MORT Measures Better" VARCHAR(10),
    "Count of MORT Measures No Different" VARCHAR(10),
    "Count of MORT Measures Worse" VARCHAR(10),
    "MORT Group Footnote" VARCHAR(1000),
    "Safety Group Measure Count" VARCHAR(10),
    "Count of Facility Safety Measures" VARCHAR(10),
    "Count of Safety Measures Better" VARCHAR(10),
    "Count of Safety Measures No Different" VARCHAR(10),
    "Count of Safety Measures Worse" VARCHAR(10),
    "Safety Group Footnote" VARCHAR(1000),
    "READM Group Measure Count" VARCHAR(10),
    "Count of Facility READM Measures" VARCHAR(10),
    "Count of READM Measures Better" VARCHAR(10),
    "Count of READM Measures No Different" VARCHAR(10),
    "Count of READM Measures Worse" VARCHAR(10),
    "READM Group Footnote" VARCHAR(1000),
    "Pt Exp Group Measure Count" VARCHAR(10),
    "Count of Facility Pt Exp Measures" VARCHAR(10),
    "Pt Exp Group Footnote" VARCHAR(1000),
    "TE Group Measure Count" VARCHAR(10),
    "Count of Facility TE Measures" VARCHAR(10),
    "TE Group Footnote" VARCHAR(1000)
)
COMMENT = 'Raw hospital general information - Master hospital list. Exact CSV structure with spaces in column names.';

-- ----------------------------------------------------------------------------
-- Table 3: Readmissions (Dataset 3)
-- Source: readmissions.csv
-- Grain: One row per hospital per readmission measure
-- ----------------------------------------------------------------------------

CREATE OR REPLACE TABLE raw.readmissions (
    "Facility Name" VARCHAR(500),
    "Facility ID" VARCHAR(50),
    "State" VARCHAR(10),
    "Measure Name" VARCHAR(200),
    "Number of Discharges" VARCHAR(50),
    "Footnote" VARCHAR(1000),
    "Excess Readmission Ratio" VARCHAR(50),
    "Predicted Readmission Rate" VARCHAR(50),
    "Expected Readmission Rate" VARCHAR(50),
    "Number of Readmissions" VARCHAR(50),
    "Start Date" VARCHAR(50),
    "End Date" VARCHAR(50)
)
COMMENT = 'Raw readmissions data - Hospital Readmissions Reduction Program. Exact CSV structure.';

-- ============================================================================
-- STEP 6: Load Data from S3 Using COPY INTO
-- ============================================================================

USE WAREHOUSE loading_wh;

-- Load IPPS Charges
COPY INTO raw.ipps_charges
FROM @healthcare_bronze_stage/ipps_charges/ipps_charges.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE'  -- Continue loading even if some rows have errors
FORCE = TRUE;  -- Overwrite existing data

-- Load Hospital General Info
COPY INTO raw.hospital_general_info
FROM @healthcare_bronze_stage/hospital_general_info/hospital_general_info.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- Load Readmissions
COPY INTO raw.readmissions
FROM @healthcare_bronze_stage/readmissions/readmissions.csv
FILE_FORMAT = (FORMAT_NAME = csv_format)
ON_ERROR = 'CONTINUE'
FORCE = TRUE;

-- ============================================================================
-- STEP 7: Verify Data Loaded
-- ============================================================================

USE DATABASE HEALTHCARE_ANALYTICS;
USE SCHEMA raw;

-- Check row counts
SELECT 'ipps_charges' AS table_name, COUNT(*) AS row_count FROM raw.ipps_charges
UNION ALL
SELECT 'hospital_general_info', COUNT(*) FROM raw.hospital_general_info
UNION ALL
SELECT 'readmissions', COUNT(*) FROM raw.readmissions;

-- Sample data from each table
SELECT * FROM raw.ipps_charges LIMIT 5;
SELECT * FROM raw.hospital_general_info LIMIT 5;
SELECT * FROM raw.readmissions LIMIT 5;

-- ============================================================================
-- STEP 8: Grant Permissions (for dbt user)
-- ============================================================================

USE DATABASE HEALTHCARE_ANALYTICS;

-- Grant usage on database and schemas
GRANT USAGE ON DATABASE HEALTHCARE_ANALYTICS TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA raw TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA staging TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA intermediate TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA marts TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA snapshots TO ROLE SYSADMIN;

-- Grant select on raw tables
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO ROLE SYSADMIN;
GRANT SELECT ON FUTURE TABLES IN SCHEMA raw TO ROLE SYSADMIN;

-- Grant create on other schemas (for dbt)
GRANT CREATE TABLE ON SCHEMA staging TO ROLE SYSADMIN;
GRANT CREATE TABLE ON SCHEMA intermediate TO ROLE SYSADMIN;
GRANT CREATE TABLE ON SCHEMA marts TO ROLE SYSADMIN;
GRANT CREATE TABLE ON SCHEMA snapshots TO ROLE SYSADMIN;

-- ============================================================================
-- Setup Complete!
-- ============================================================================
-- 
-- Next steps:
-- 1. Verify data loaded correctly (check row counts above)
-- 2. Update dbt profiles.yml with your Snowflake credentials
-- 3. Run: dbt deps (install packages)
-- 4. Run: dbt run (build all models)
-- 5. Run: dbt test (validate data quality)
-- ============================================================================

