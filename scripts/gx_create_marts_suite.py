#!/usr/bin/env python3
"""
Create Great Expectations Expectation Suite for Marts Layer
Starts with fct_inpatient_charges
"""

import sys
from pathlib import Path

try:
    import great_expectations as gx
except ImportError:
    print("ERROR: Great Expectations not installed.")
    sys.exit(1)

def main():
    project_root = Path.cwd()
    context = gx.get_context()
    
    print("=" * 60)
    print("Create Expectation Suite for fct_inpatient_charges")
    print("=" * 60)
    print()
    
    # Check if datasource exists
    try:
        datasource = context.fluent_datasources["snowflake_datasource"]
        print(f"Found datasource: {datasource.name}")
    except KeyError:
        print("ERROR: Snowflake datasource not configured.")
        print("Run: python scripts/gx_configure_snowflake_working.py first")
        sys.exit(1)
    
    suite_name = "marts.fct_inpatient_charges"
    
    # Check if suite already exists
    try:
        existing_suite = context.get_expectation_suite(suite_name)
        print(f"WARNING: Suite '{suite_name}' already exists!")
        response = input("Do you want to replace it? (y/N): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
        # Delete existing suite
        context.delete_expectation_suite(suite_name)
    except:
        pass  # Suite doesn't exist, which is fine
    
    print()
    print("Creating data asset...")
    
    # Create data asset for fct_inpatient_charges using fluent API
    try:
        # Check if asset already exists
        existing_asset_names = [asset.name for asset in datasource.assets] if hasattr(datasource, 'assets') else []
        if "fct_inpatient_charges" in existing_asset_names:
            print("Asset 'fct_inpatient_charges' already exists. Using existing asset.")
            # Find the asset by name
            asset = next((a for a in datasource.assets if a.name == "fct_inpatient_charges"), None)
            if asset is None:
                raise Exception("Asset found in list but couldn't retrieve it")
        else:
            # Create new asset using fluent API - use add_table_asset method
            # Note: dbt creates tables in raw_marts schema (due to profiles.yml default schema)
            print("Creating data asset (this will test the connection to the table)...")
            print("Note: Looking for table in 'raw_marts' schema (dbt default)...")
            asset = datasource.add_table_asset(
                name="fct_inpatient_charges",
                table_name="fct_inpatient_charges",
                schema_name="raw_marts"  # dbt creates tables in raw_marts, not marts
            )
            print(f"SUCCESS: Data asset created: {asset.name}")
    except Exception as e:
        error_msg = str(e)
        if "does not exist" in error_msg or "not authorized" in error_msg:
            print("\n" + "=" * 60)
            print("ERROR: Table does not exist in Snowflake!")
            print("=" * 60)
            print("\nThe table 'fct_inpatient_charges' needs to be created first.")
            print("\nPlease run dbt to build the marts models:")
            print("  dbt run --select fct_inpatient_charges")
            print("\nOr run all marts models:")
            print("  dbt run --select marts.*")
            print("\nAfter the table exists, run this script again.")
        else:
            print(f"ERROR: Failed to create data asset: {e}")
            import traceback
            traceback.print_exc()
        sys.exit(1)
    
    print()
    print("Creating expectation suite and validator...")
    
    # Build batch request
    batch_request = asset.build_batch_request()
    
    # Check if suite exists, if not create it
    try:
        existing_suite = context.suites.get(suite_name)
        print(f"Suite '{suite_name}' already exists. Using existing suite.")
    except:
        # Create new ExpectationSuite object
        from great_expectations.core import ExpectationSuite
        suite = ExpectationSuite(name=suite_name)
        context.suites.add_or_update(suite)
        print(f"Created new suite: {suite_name}")
    
    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print()
    print("Adding expectations...")
    
    # Add expectations
    expectations_added = []
    
    try:
        # Primary key uniqueness
        validator.expect_column_values_to_be_unique(column="charge_key")
        expectations_added.append("charge_key uniqueness")
        
        # Not null checks for key columns (hospital_key may be NULL for orphaned records)
        validator.expect_column_values_to_not_be_null(column="charge_key")
        # Note: hospital_key can be NULL for orphaned records (hospitals not in dim_hospitals)
        # We track these with has_orphaned_hospital flag instead
        validator.expect_column_values_to_not_be_null(column="drg_key")
        expectations_added.append("key columns not null (except hospital_key)")
        
        # Value ranges
        validator.expect_column_values_to_be_between(
            column="avg_covered_charges",
            min_value=0,
            max_value=9999999
        )
        validator.expect_column_values_to_be_between(
            column="markup_ratio",
            min_value=1.0,
            max_value=100.0
        )
        expectations_added.append("value ranges")
        
        # Data quality flags
        validator.expect_column_values_to_be_in_set(
            column="data_quality_flag_covered_charges_issue",
            value_set=[True, False]
        )
        validator.expect_column_values_to_be_in_set(
            column="data_quality_flag_capped_markup_ratio",
            value_set=[True, False]
        )
        validator.expect_column_values_to_be_in_set(
            column="has_orphaned_hospital",
            value_set=[True, False]
        )
        # Note: Percentage monitoring (< 5% orphaned) is handled by dbt test in schema.yml
        # This expectation just ensures the flag column exists and has valid boolean values
        validator.expect_column_values_to_be_in_set(
            column="has_data_quality_issues",
            value_set=[True, False]
        )
        expectations_added.append("data quality flags")
        
        # Row count (approximate)
        validator.expect_table_row_count_to_be_between(
            min_value=140000,
            max_value=150000
        )
        expectations_added.append("row count")
        
        # Save suite (use add_or_update to handle existing suites)
        suite_to_save = validator.get_expectation_suite()
        context.suites.add_or_update(suite_to_save)
        
        print()
        print("SUCCESS: Expectation suite created!")
        print(f"Suite name: {suite_name}")
        print(f"Expectations added: {len(expectations_added)}")
        for exp in expectations_added:
            print(f"  - {exp}")
        print()
        print("Next steps:")
        print("  1. Create checkpoint: python scripts/gx_create_checkpoint.py")
        print("  2. Run validation: python scripts/gx_run_checkpoint.py")
        
    except Exception as e:
        print(f"\nERROR: Failed to add expectations: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

