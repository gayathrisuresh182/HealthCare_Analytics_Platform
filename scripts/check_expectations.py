#!/usr/bin/env python3
"""Check what expectations are in the suite"""

import great_expectations as gx

context = gx.get_context()
suite = context.suites.get('marts.fct_inpatient_charges')

print("Expectations in suite 'marts.fct_inpatient_charges':")
print("=" * 60)
for i, exp in enumerate(suite.expectations, 1):
    print(f"{i}. {exp}")
    print()

