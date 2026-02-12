#!/usr/bin/env python3
"""
Simple Snowflake Datasource Configuration for Great Expectations
Uses legacy API (works with GX v1.11.3)
"""

import sys
import yaml
from pathlib import Path

try:
    import great_expectations as gx
except ImportError:
    print("ERROR: Great Expectations not installed.")
    sys.exit(1)

def read_dbt_profiles():
    """Try to read credentials from dbt profiles"""
    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    if not profiles_path.exists():
        return None
    
    try:
        with open(profiles_path, 'r') as f:
            profiles = yaml.safe_load(f)
        
        # Look for healthcare_analytics profile
        if 'healthcare_analytics' in profiles:
            dev = profiles['healthcare_analytics']['outputs'].get('dev', {})
            return {
                'account': dev.get('account', ''),
                'user': dev.get('user', ''),
                'password': dev.get('password', ''),
                'database': dev.get('database', 'HEALTHCARE_ANALYTICS'),
                'warehouse': dev.get('warehouse', 'transforming_wh'),
                'role': dev.get('role', 'SYSADMIN')
            }
    except Exception as e:
        print(f"Could not read dbt profiles: {e}")
    
    return None

def main():
    context = gx.get_context()
    
    print("=" * 60)
    print("Configure Great Expectations Snowflake Datasource")
    print("=" * 60)
    print()
    
    # Try to read from dbt profiles
    dbt_creds = read_dbt_profiles()
    
    if dbt_creds and all([dbt_creds.get('account'), dbt_creds.get('user'), dbt_creds.get('password')]):
        print("Found dbt profiles! Using those credentials.")
        print(f"  Account: {dbt_creds['account']}")
        print(f"  User: {dbt_creds['user']}")
        print(f"  Database: {dbt_creds['database']}")
        print(f"  Warehouse: {dbt_creds['warehouse']}")
        print()
        use_dbt = input("Use these credentials? (Y/n): ").strip().lower()
        if use_dbt != 'n':
            account = dbt_creds['account']
            user = dbt_creds['user']
            password = dbt_creds['password']
            database = dbt_creds['database']
            warehouse = dbt_creds['warehouse']
            role = dbt_creds['role']
        else:
            dbt_creds = None
    else:
        dbt_creds = None
    
    if not dbt_creds:
        print("Enter Snowflake connection details:")
        print()
        account = input("Account Locator: ").strip()
        user = input("Username: ").strip()
        password = input("Password: ").strip()
        database = input(f"Database [HEALTHCARE_ANALYTICS]: ").strip() or "HEALTHCARE_ANALYTICS"
        warehouse = input(f"Warehouse [transforming_wh]: ").strip() or "transforming_wh"
        role = input(f"Role [SYSADMIN]: ").strip() or "SYSADMIN"
    
    if not all([account, user, password]):
        print("\nERROR: Account, username, and password are required.")
        sys.exit(1)
    
    # Check if datasource exists
    try:
        datasources = context.list_datasources()
        if any(ds.get("name") == "snowflake_datasource" for ds in datasources):
            print("\nWARNING: Datasource 'snowflake_datasource' already exists!")
            response = input("Replace it? (y/N): ").strip().lower()
            if response != 'y':
                print("Cancelled.")
                return
            # Delete existing
            try:
                context.delete_datasource("snowflake_datasource")
            except:
                pass
    except:
        pass
    
    # Install snowflake-sqlalchemy if needed
    try:
        import snowflake.sqlalchemy
    except ImportError:
        print("\nInstalling snowflake-sqlalchemy...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "snowflake-sqlalchemy"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("Installed!")
    
    print()
    print("Creating Snowflake datasource...")
    
    try:
        # Create connection string
        connection_string = f"snowflake://{user}:{password}@{account}/{database}?warehouse={warehouse}&role={role}"
        
        # Use legacy add_datasource API (works in v1.11.3)
        datasource = context.add_datasource(
            name="snowflake_datasource",
            class_name="SqlAlchemyDatasource",
            connection_string=connection_string
        )
        
        print()
        print("SUCCESS: Snowflake datasource created!")
        print(f"  Name: snowflake_datasource")
        print(f"  Database: {database}")
        print(f"  Warehouse: {warehouse}")
        print()
        print("Next steps:")
        print("  1. Create expectation suite: python scripts/gx_create_marts_suite.py")
        print("  2. Create checkpoint: python scripts/gx_create_checkpoint.py")
        print("  3. Run validation: python scripts/gx_run_checkpoint.py")
        
    except Exception as e:
        print(f"\nERROR: Failed to create datasource: {e}")
        print("\nTroubleshooting:")
        print("  - Verify Snowflake credentials are correct")
        print("  - Check network connectivity to Snowflake")
        print("  - Ensure warehouse is running")
        print("  - Try: pip install snowflake-sqlalchemy")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

