#!/usr/bin/env python3
"""
Great Expectations Docs Build - Wrapper script
Replaces: great_expectations docs build
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
    
    # GX might create 'gx' or 'great_expectations' directory
    gx_dir = project_root / "great_expectations"
    gx_dir_alt = project_root / "gx"
    
    if not gx_dir.exists() and not gx_dir_alt.exists():
        print("ERROR: Great Expectations not initialized.")
        print("Run: python scripts/gx_init.py first")
        sys.exit(1)
    
    # Use whichever directory exists
    actual_gx_dir = gx_dir if gx_dir.exists() else gx_dir_alt
    
    print("Building Great Expectations data docs...")
    context = gx.get_context()
    context.build_data_docs()
    print("Data docs built successfully!")
    print(f"View at: {actual_gx_dir}/uncommitted/data_docs/local_site/index.html")

if __name__ == "__main__":
    main()

