#!/usr/bin/env python3
"""
Simple Great Expectations initialization script
Uses Python API directly (works on Windows)
"""

import os
import sys
from pathlib import Path

try:
    import great_expectations as gx
except ImportError:
    print("❌ Great Expectations not installed.")
    print("   Run: pip install great-expectations")
    sys.exit(1)

def main():
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    
    print("Initializing Great Expectations...")
    print(f"Working directory: {os.getcwd()}\n")
    
    # Check if already exists
    gx_dir = project_root / "great_expectations"
    if gx_dir.exists():
        print(f"Great Expectations already initialized at: {gx_dir}")
        print("   To reinitialize, delete the 'great_expectations' folder first.")
        return
    
    try:
        # Initialize GX context (this creates the directory structure)
        print("Creating Great Expectations context...")
        context = gx.get_context()
        
        print("Great Expectations initialized successfully!")
        print(f"Created at: {gx_dir}")
        print("\nNext steps:")
        print("   1. Configure Snowflake datasource")
        print("   2. Create expectation suites")
        print("   3. Set up checkpoints")
        print("\nSee: docs/great_expectations_setup.md for detailed guide")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

