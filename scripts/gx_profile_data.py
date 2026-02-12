#!/usr/bin/env python3
"""
Generate data profiles for Great Expectations data docs
This adds statistical summaries, distributions, and data quality metrics
"""

import sys
import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.datasource.fluent import BatchRequest

def main():
    print("=" * 60)
    print("Generate Data Profiles for fct_inpatient_charges")
    print("=" * 60)
    print()
    
    context = gx.get_context()
    
    # Get datasource and asset
    datasource = context.fluent_datasources["snowflake_datasource"]
    asset = datasource.get_asset("fct_inpatient_charges")
    batch_request = asset.build_batch_request()
    
    # Get validator
    suite_name = "marts.fct_inpatient_charges"
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    print("Generating data profiles...")
    print("This will add statistical summaries to the data docs.")
    print()
    
    # Add profiling expectations (these generate statistics)
    # Note: Some of these are already in the suite, but we'll add more detailed ones
    
    print("Adding profiling expectations...")
    
    # Column statistics
    try:
        # These will generate statistics that appear in data docs
        validator.expect_column_mean_to_be_between(
            column="avg_covered_charges",
            min_value=0,
            max_value=9999999,
            mostly=0.95  # Allow some flexibility
        )
        print("  ✓ Added mean expectation for avg_covered_charges")
    except Exception as e:
        print(f"  ⚠ Could not add mean expectation: {e}")
    
    try:
        validator.expect_column_median_to_be_between(
            column="avg_covered_charges",
            min_value=0,
            max_value=9999999,
            mostly=0.95
        )
        print("  ✓ Added median expectation for avg_covered_charges")
    except Exception as e:
        print(f"  ⚠ Could not add median expectation: {e}")
    
    # Value distribution
    try:
        validator.expect_column_values_to_be_in_type_list(
            column="has_orphaned_hospital",
            type_list=["BOOLEAN"]
        )
        print("  ✓ Added type expectation for has_orphaned_hospital")
    except Exception as e:
        print(f"  ⚠ Could not add type expectation: {e}")
    
    # Save the suite
    suite_to_save = validator.get_expectation_suite()
    context.suites.add_or_update(suite_to_save)
    
    print()
    print("SUCCESS: Data profiling expectations added!")
    print()
    print("Next steps:")
    print("  1. Run validation: python scripts/gx_run_checkpoint.py")
    print("  2. Rebuild docs: python scripts/gx_docs_build.py")
    print("  3. Check data docs for statistical summaries")

if __name__ == "__main__":
    main()

