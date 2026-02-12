#!/usr/bin/env python3
"""
Generate data quality scorecard from Great Expectations validation results
Creates a summary report with quality scores and trends
"""

import sys
import json
from pathlib import Path
from datetime import datetime
import great_expectations as gx

def calculate_quality_score(validation_result):
    """Calculate overall data quality score (0-100)"""
    total_expectations = len(validation_result.results)
    passed_expectations = sum(1 for r in validation_result.results if r.success)
    
    if total_expectations == 0:
        return 0
    
    score = (passed_expectations / total_expectations) * 100
    return round(score, 2)

def calculate_category_scores(validation_result):
    """Calculate scores by category"""
    categories = {
        'completeness': [],
        'validity': [],
        'uniqueness': [],
        'consistency': []
    }
    
    for result in validation_result.results:
        exp_type = result.expectation_config.get('expectation_type', '')
        
        if 'not_null' in exp_type or 'null' in exp_type:
            categories['completeness'].append(result)
        elif 'unique' in exp_type:
            categories['uniqueness'].append(result)
        elif 'between' in exp_type or 'in_set' in exp_type:
            categories['validity'].append(result)
        else:
            categories['consistency'].append(result)
    
    category_scores = {}
    for category, results in categories.items():
        if len(results) == 0:
            category_scores[category] = None
        else:
            passed = sum(1 for r in results if r.success)
            category_scores[category] = round((passed / len(results)) * 100, 2) if len(results) > 0 else None
    
    return category_scores

def main():
    print("=" * 60)
    print("Generate Data Quality Scorecard")
    print("=" * 60)
    print()
    
    context = gx.get_context()
    
    # Run a fresh validation to get results
    print("Running validation to get latest results...")
    
    datasource = context.fluent_datasources["snowflake_datasource"]
    asset = datasource.get_asset("fct_inpatient_charges")
    batch_request = asset.build_batch_request()
    suite_name = "marts.fct_inpatient_charges"
    
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name
    )
    
    # Run validation
    validation_result = validator.validate()
    
    # Extract results
    results = validation_result.results
    total_expectations = len(results)
    passed = sum(1 for r in results if r.success)
    failed = total_expectations - passed
    
    # Calculate scores
    overall_score = round((passed / total_expectations) * 100, 2) if total_expectations > 0 else 0
    
    print()
    print("=" * 60)
    print("Data Quality Scorecard")
    print("=" * 60)
    print()
    print(f"Overall Quality Score: {overall_score}%")
    print(f"  Passed: {passed}/{total_expectations}")
    print(f"  Failed: {failed}/{total_expectations}")
    print()
    
    # Show failed expectations
    failed_expectations = [r for r in results if not r.success]
    if failed_expectations:
        print("Failed Expectations:")
        for exp in failed_expectations:
            exp_type = exp.expectation_config.get('expectation_type', 'Unknown')
            column = exp.expectation_config.get('kwargs', {}).get('column', 'N/A')
            print(f"  - {exp_type} on {column}")
    else:
        print("[OK] All expectations passed!")
    
    print()
    print("=" * 60)
    print("Scorecard Summary")
    print("=" * 60)
    print()
    print(f"Quality Level: ", end="")
    if overall_score >= 95:
        print("[EXCELLENT]")
    elif overall_score >= 90:
        print("[GOOD]")
    elif overall_score >= 80:
        print("[ACCEPTABLE]")
    else:
        print("[NEEDS IMPROVEMENT]")
    
    print()
    print("Recommendations:")
    if overall_score >= 95:
        print("  [OK] Data quality is excellent. Maintain current standards.")
    elif overall_score >= 90:
        print("  [WARN] Data quality is good. Review failed expectations.")
    else:
        print("  [ERROR] Data quality needs improvement. Investigate failed expectations.")
    
    # Save scorecard
    scorecard_path = Path(context.root_directory) / "uncommitted" / "scorecard.json"
    scorecard_path.parent.mkdir(parents=True, exist_ok=True)
    scorecard_data = {
        "timestamp": datetime.now().isoformat(),
        "overall_score": overall_score,
        "total_expectations": total_expectations,
        "passed": passed,
        "failed": failed,
        "suite_name": suite_name
    }
    
    with open(scorecard_path, 'w') as f:
        json.dump(scorecard_data, f, indent=2)
    
    print()
    print(f"Scorecard saved to: {scorecard_path}")

if __name__ == "__main__":
    main()

