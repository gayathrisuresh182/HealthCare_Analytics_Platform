#!/usr/bin/env python3
"""
Configure Snowflake Datasource using Fluent API (GX v1.11.3)
"""

import sys
import yaml
from pathlib import Path

try:
    import great_expectations as gx
    from great_expectations.datasource.fluent import SnowflakeDatasource
except ImportError:
    print("ERROR: Great Expectations not installed.")
    sys.exit(1)

def read_dbt_profiles():
    """Read credentials from dbt profiles"""
    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    if not profiles_path.exists():
        return None
    
    try:
        with open(profiles_path, 'r') as f:
            profiles = yaml.safe_load(f)
        
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
        print(f"ERROR: Could not read dbt profiles: {e}")
    
    return None

def main():
    context = gx.get_context()
    
    print("=" * 60)
    print("Configure Great Expectations Snowflake Datasource")
    print("=" * 60)
    print()
    
    # Read from dbt profiles
    creds = read_dbt_profiles()
    
    if not creds or not all([creds.get('account'), creds.get('user'), creds.get('password')]):
        print("ERROR: Could not read Snowflake credentials from dbt profiles.")
        print(f"Expected file: {Path.home() / '.dbt' / 'profiles.yml'}")
        sys.exit(1)
    
    print("Using credentials from dbt profiles:")
    print(f"  Account: {creds['account']}")
    print(f"  User: {creds['user']}")
    print(f"  Database: {creds['database']}")
    print(f"  Warehouse: {creds['warehouse']}")
    print()
    
    # Check if datasource exists
    if "snowflake_datasource" in context.fluent_datasources:
        print("Datasource 'snowflake_datasource' already exists. Replacing...")
        try:
            context._delete_fluent_datasource("snowflake_datasource")
        except:
            pass
    
    # Install snowflake-sqlalchemy if needed
    try:
        import snowflake.sqlalchemy
    except ImportError:
        print("Installing snowflake-sqlalchemy...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "snowflake-sqlalchemy"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print("Installed!")
    
    print("Creating Snowflake datasource...")
    
    try:
        # Use fluent API - create SnowflakeDatasource with individual parameters
        datasource = SnowflakeDatasource(
            name="snowflake_datasource",
            account=creds['account'],
            user=creds['user'],
            password=creds['password'],
            database=creds['database'],
            schema="marts",  # Default schema, can be changed per asset
            warehouse=creds['warehouse'],
            role=creds['role']
        )
        
        # Add to context
        context._add_fluent_datasource(datasource)
        
        # Save the configuration
        context._save_project_config()
        
        print()
        print("SUCCESS: Snowflake datasource created!")
        print(f"  Name: snowflake_datasource")
        print(f"  Database: {creds['database']}")
        print(f"  Warehouse: {creds['warehouse']}")
        print()
        print("Next steps:")
        print("  1. Create expectation suite: python scripts/gx_create_marts_suite.py")
        print("  2. Create checkpoint: python scripts/gx_create_checkpoint.py")
        print("  3. Run validation: python scripts/gx_run_checkpoint.py")
        
    except Exception as e:
        print(f"\nERROR: Failed to create datasource: {e}")
        print("\nTroubleshooting:")
        print("  - Verify Snowflake credentials in dbt profiles")
        print("  - Check network connectivity")
        print("  - Ensure warehouse is running")
        print("  - Try: pip install snowflake-sqlalchemy")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

