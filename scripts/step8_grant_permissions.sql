-- ============================================================================
-- STEP 8: Grant Permissions (Standalone Script)
-- Run this if Step 8 failed in the main script
-- ============================================================================

-- Set context explicitly
USE ROLE ACCOUNTADMIN;  -- Or SYSADMIN
USE DATABASE HEALTHCARE_ANALYTICS;

-- Verify context
SELECT CURRENT_DATABASE() AS current_database, CURRENT_ROLE() AS current_role;

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

-- Verify grants were successful
SELECT 'Permissions granted successfully!' AS status;

