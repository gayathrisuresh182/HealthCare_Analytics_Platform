#!/usr/bin/env python3
"""Delete the expectation suite to start fresh"""

import great_expectations as gx

context = gx.get_context()
suite_name = "marts.fct_inpatient_charges"

try:
    suite = context.suites.get(suite_name)
    context.suites.delete(suite_name)
    print(f"SUCCESS: Deleted suite '{suite_name}'")
except Exception as e:
    print(f"ERROR: {e}")

