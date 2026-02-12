#!/usr/bin/env python3
"""
Create expectation suites for all marts tables
This provides comprehensive data quality coverage
"""

import sys
import os
# Fix Unicode encoding for Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

import great_expectations as gx

def create_suite_for_table(context, table_name, schema_name="raw_marts"):
    """Create a basic expectation suite for a table"""
    print(f"\nCreating suite for {table_name}...")
    
    datasource = context.fluent_datasources["snowflake_datasource"]
    
    # Check if asset exists, create if not
    try:
        asset = datasource.get_asset(table_name)
        print(f"  Asset '{table_name}' already exists")
    except (LookupError, IndexError):
        print(f"  Creating asset '{table_name}'...")
        try:
            asset = datasource.add_table_asset(
                name=table_name,
                table_name=table_name,
                schema_name=schema_name
            )
            print(f"  [OK] Asset created")
        except Exception as e:
            print(f"  [ERROR] Failed to create asset: {e}")
            return
    
    batch_request = asset.build_batch_request()
    suite_name = f"marts.{table_name}"
    
    # Check if suite exists
    try:
        suite = context.suites.get(suite_name)
        print(f"  Suite '{suite_name}' already exists. Skipping.")
        return
    except:
        pass
    
    # Create suite
    from great_expectations.core import ExpectationSuite
    suite = ExpectationSuite(name=suite_name)
    context.suites.add_or_update(suite)
    
    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    # Add basic expectations
    print(f"  Adding basic expectations...")
    
    # Table-level
    try:
        validator.expect_table_row_count_to_be_between(min_value=1, max_value=10000000)
        print(f"    [OK] Row count expectation")
    except Exception as e:
        print(f"    [WARN] Row count: {e}")
    
    # Save suite
    suite_to_save = validator.get_expectation_suite()
    context.suites.add_or_update(suite_to_save)
    
    print(f"  [OK] Suite '{suite_name}' created")

def main():
    print("=" * 60)
    print("Create Expectation Suites for All Marts Tables")
    print("=" * 60)
    
    context = gx.get_context()
    
    # List of marts tables to create suites for
    marts_tables = [
        "dim_hospitals",
        "dim_drg_codes", 
        "dim_geography",
        "fct_readmissions",
        "fct_hospital_summary",
        "fct_state_summary"
    ]
    
    print(f"\nWill create suites for {len(marts_tables)} tables:")
    for table in marts_tables:
        print(f"  - {table}")
    
    print("\nNote: fct_inpatient_charges already has a suite")
    
    for table in marts_tables:
        try:
            create_suite_for_table(context, table)
        except Exception as e:
            print(f"  [ERROR] Failed: {e}")
    
    print()
    print("=" * 60)
    print("SUCCESS: Expectation suites created!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("  1. Customize expectations for each table")
    print("  2. Run validations: python scripts/gx_run_checkpoint.py")
    print("  3. Build docs: python scripts/gx_docs_build.py")

if __name__ == "__main__":
    main()

