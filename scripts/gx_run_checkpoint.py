#!/usr/bin/env python3
"""
Run Great Expectations Checkpoint
Replaces: great_expectations checkpoint run
"""

import sys
from pathlib import Path

try:
    import great_expectations as gx
except ImportError:
    print("ERROR: Great Expectations not installed.")
    sys.exit(1)

def main():
    context = gx.get_context()
    
    checkpoint_name = "marts_checkpoint"
    
    print("=" * 60)
    print(f"Running Checkpoint: {checkpoint_name}")
    print("=" * 60)
    print()
    
    # Get datasource and asset to build batch request
    try:
        datasource = context.fluent_datasources["snowflake_datasource"]
        asset = datasource.get_asset("fct_inpatient_charges")
        batch_request = asset.build_batch_request()
        suite_name = "marts.fct_inpatient_charges"
    except Exception as e:
        print(f"ERROR: Could not get datasource/asset: {e}")
        sys.exit(1)
    
    print("Running validation directly (bypassing checkpoint)...")
    print()
    
    try:
        # Run validation directly using validator
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=suite_name
        )
        
        # Run validation
        result = validator.validate()
        
        print("=" * 60)
        print("Validation Results")
        print("=" * 60)
        print()
        
        if result.success:
            print("SUCCESS: All expectations passed!")
        else:
            print("WARNING: Some expectations failed.")
            failed_count = result.statistics.get('evaluated_expectations', 0) - result.statistics.get('successful_expectations', 0)
            print(f"   Failed expectations: {failed_count}")
            
            # Show which expectations failed
            print("\nFailed expectations:")
            for result_item in result.results:
                if not result_item.success:
                    exp_config = result_item.expectation_config
                    exp_type = getattr(exp_config, 'expectation_type', None) or getattr(exp_config, 'expectationContext', {}).get('expectation_type', 'Unknown')
                    exp_kwargs = getattr(exp_config, 'kwargs', {})
                    print(f"   - {exp_type}")
                    if 'column' in exp_kwargs:
                        print(f"     Column: {exp_kwargs['column']}")
                    if hasattr(result_item, 'result') and result_item.result:
                        if 'observed_value' in result_item.result:
                            print(f"     Observed: {result_item.result.get('observed_value')}")
                        if 'element_count' in result_item.result:
                            print(f"     Total rows: {result_item.result.get('element_count')}")
        
        print()
        print("Statistics:")
        stats = result.statistics
        print(f"  Total expectations: {stats.get('evaluated_expectations', 0)}")
        print(f"  Successful: {stats.get('successful_expectations', 0)}")
        print(f"  Failed: {stats.get('unsuccessful_expectations', 0)}")
        print()
        print("Validation results saved to: gx/uncommitted/validations/")
        print()
        print("Next step:")
        print("  Build data docs: python scripts/gx_docs_build.py")
        
    except Exception as e:
        print(f"\nERROR: Failed to run checkpoint: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

