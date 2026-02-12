#!/usr/bin/env python3
"""
Great Expectations Init - Wrapper script
Replaces: great_expectations init
"""

import sys
from pathlib import Path

try:
    import great_expectations as gx
except ImportError:
    print("ERROR: Great Expectations not installed.")
    print("Run: pip install great-expectations")
    sys.exit(1)

def main():
    project_root = Path.cwd()
    
    # GX might create 'gx' or 'great_expectations' directory
    gx_dir = project_root / "great_expectations"
    gx_dir_alt = project_root / "gx"
    
    if gx_dir.exists() or gx_dir_alt.exists():
        existing = gx_dir if gx_dir.exists() else gx_dir_alt
        print(f"Great Expectations already initialized at: {existing}")
        return
    
    print("Initializing Great Expectations...")
    context = gx.get_context(mode='file')
    actual_dir = context.root_directory if hasattr(context, 'root_directory') else (gx_dir if gx_dir.exists() else gx_dir_alt)
    print(f"Success! Created at: {actual_dir}")

if __name__ == "__main__":
    main()

