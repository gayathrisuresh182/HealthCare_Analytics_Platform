#!/usr/bin/env python3
"""
Investigate why hospital_key has NULL values in fct_inpatient_charges
"""

import sys
try:
    import great_expectations as gx
    from great_expectations.datasource.fluent import SnowflakeDatasource
except ImportError:
    print("ERROR: Great Expectations not installed.")
    sys.exit(1)

def main():
    context = gx.get_context()
    
    print("=" * 60)
    print("Investigate hospital_key NULL Values")
    print("=" * 60)
    print()
    
    # Get datasource and asset
    datasource = context.fluent_datasources["snowflake_datasource"]
    asset = datasource.get_asset("fct_inpatient_charges")
    batch_request = asset.build_batch_request()
    
    # Get validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="marts.fct_inpatient_charges"
    )
    
    print("Querying data to investigate NULL hospital_key values...")
    print()
    
    # Query for NULL hospital_key values
    # Note: We'll use a simple query approach
    print("Checking for NULL hospital_key values...")
    
    # Get batch data
    batch = validator.active_batch
    df = batch.data.dataframe
    
    # Check for NULLs
    null_count = df['hospital_key'].isna().sum()
    total_count = len(df)
    
    print(f"Total rows: {total_count:,}")
    print(f"NULL hospital_key: {null_count:,}")
    print(f"Percentage NULL: {(null_count/total_count)*100:.2f}%")
    
    if null_count > 0:
        print()
        print("Sample rows with NULL hospital_key:")
        null_rows = df[df['hospital_key'].isna()][['hospital_id', 'drg_code', 'charge_key']].head(10)
        print(null_rows.to_string())
        print()
        print("Possible causes:")
        print("  1. LEFT JOIN in fct_inpatient_charges allows NULLs")
        print("  2. hospital_id in charges doesn't match facility_id in dim_hospitals")
        print("  3. dim_hospitals WHERE is_current = TRUE filters out some hospitals")
    
    print()
    print("Checking join logic in fct_inpatient_charges.sql...")
    print("The model uses LEFT JOIN which can produce NULL hospital_key values")
    print("if hospital_id doesn't match facility_id in dim_hospitals")

if __name__ == "__main__":
    main()

