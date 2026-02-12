#!/usr/bin/env python3
"""
Configure Great Expectations Snowflake Datasource
Replaces: great_expectations datasource new
"""

import sys
import os
from pathlib import Path

try:
    import great_expectations as gx
except ImportError:
    print("ERROR: Great Expectations not installed.")
    sys.exit(1)

def main():
    project_root = Path.cwd()
    
    # Check if GX is initialized
    gx_dir = project_root / "gx"
    gx_dir_alt = project_root / "great_expectations"
    
    if not gx_dir.exists() and not gx_dir_alt.exists():
        print("ERROR: Great Expectations not initialized.")
        print("Run: python scripts/gx_init.py first")
        sys.exit(1)
    
    print("=" * 60)
    print("Configure Great Expectations Snowflake Datasource")
    print("=" * 60)
    print()
    
    # Get context
    context = gx.get_context()
    
    # Check if datasource already exists
    datasource_exists = False
    try:
        # Check legacy datasources (using data_sources instead of deprecated get_datasource)
        datasources = context.list_datasources()
        if any(ds.get("name") == "snowflake_datasource" for ds in datasources):
            datasource_exists = True
    except:
        pass
    
    if datasource_exists:
        print("WARNING: Datasource 'snowflake_datasource' already exists!")
        try:
            response = input("Do you want to replace it? (y/N): ")
            if response.lower() != 'y':
                print("Cancelled.")
                return
        except EOFError:
            # Non-interactive mode - skip confirmation
            print("Datasource exists. Use --force to replace.")
            return
    
    print()
    print("Enter your Snowflake connection details:")
    print("(Press Enter to use defaults from dbt profiles)")
    print()
    
    # Try to read from dbt profiles
    dbt_profiles_path = Path.home() / ".dbt" / "profiles.yml"
    account = None
    user = None
    password = None
    database = "HEALTHCARE_ANALYTICS"
    warehouse = "transforming_wh"
    role = "SYSADMIN"
    
    if dbt_profiles_path.exists():
        print(f"Found dbt profiles at: {dbt_profiles_path}")
        print("You can use the same credentials from your dbt setup.")
        print()
    
    # Get connection details
    try:
        account = input(f"Account Locator [{account or 'YOUR_ACCOUNT'}]: ").strip() or account
        user = input(f"Username [{user or 'YOUR_USERNAME'}]: ").strip() or user
        password = input(f"Password: ").strip() or password
        database = input(f"Database [{database}]: ").strip() or database
        warehouse = input(f"Warehouse [{warehouse}]: ").strip() or warehouse
        role = input(f"Role [{role}]: ").strip() or role
    except EOFError:
        print("\nERROR: Cannot read input in non-interactive mode.")
        print("Please run this script in an interactive terminal.")
        print("\nOr set environment variables:")
        print("  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc.")
        sys.exit(1)
    
    if not all([account, user, password]):
        print("\nERROR: Account, username, and password are required.")
        sys.exit(1)
    
    print()
    print("Creating Snowflake datasource...")
    
    try:
        # Check if snowflake-sqlalchemy is installed
        try:
            import snowflake.sqlalchemy
        except ImportError:
            print("\nWARNING: snowflake-sqlalchemy not installed.")
            print("Installing it now...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "snowflake-sqlalchemy"], 
                                 stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print("Installed successfully!")
        
        # GX v1.11.3 uses legacy add_datasource API
        # Create connection string
        connection_string = f"snowflake://{user}:{password}@{account}/{database}?warehouse={warehouse}&role={role}"
        
        # Use legacy add_datasource method (works in v1.11.3)
        print("Adding Snowflake datasource...")
        datasource = context.add_datasource(
            name="snowflake_datasource",
            class_name="SqlAlchemyDatasource",
            connection_string=connection_string
        )
        
        print("SUCCESS: Snowflake datasource created!")
        print(f"Name: snowflake_datasource")
        print(f"Database: {database}")
        print()
        print("Next steps:")
        print("  1. Create expectation suite: python scripts/gx_create_marts_suite.py")
        print("  2. Create checkpoint: python scripts/gx_create_checkpoint.py")
        print("  3. Run validation: python scripts/gx_run_checkpoint.py")
        
    except Exception as e:
        print(f"\nERROR: Failed to create datasource: {e}")
        print("\nTroubleshooting:")
        print("  - Verify Snowflake credentials")
        print("  - Check network connectivity")
        print("  - Ensure warehouse is running")
        print("  - Make sure snowflake-sqlalchemy is installed: pip install snowflake-sqlalchemy")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

