#!/usr/bin/env python3
"""
Check if missing hospital_ids exist in source data (stg_hospitals)
"""

import sys
import yaml
from pathlib import Path
from sqlalchemy import create_engine, text

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
                'role': dev.get('role', 'ACCOUNTADMIN')
            }
    except Exception as e:
        print(f"ERROR: Could not read dbt profiles: {e}")
    
    return None

def main():
    print("=" * 60)
    print("Check Missing Hospitals in Source Data")
    print("=" * 60)
    print()
    
    creds = read_dbt_profiles()
    if not creds:
        print("ERROR: Could not read dbt profiles")
        sys.exit(1)
    
    connection_string = f"snowflake://{creds['user']}:{creds['password']}@{creds['account']}/{creds['database']}?warehouse={creds['warehouse']}&role={creds['role']}"
    engine = create_engine(connection_string)
    
    # Query: Check if missing hospital_ids exist in stg_hospitals
    query = """
    WITH missing_hospitals AS (
        SELECT DISTINCT hospital_id
        FROM raw_marts.fct_inpatient_charges
        WHERE hospital_key IS NULL
    )
    SELECT 
        m.hospital_id,
        CASE 
            WHEN s.facility_id IS NOT NULL THEN 'EXISTS in stg_hospitals'
            ELSE 'MISSING from stg_hospitals (not in source data)'
        END AS source_status,
        s.facility_name,
        s.state,
        COUNT(DISTINCT c.charge_key) AS charge_records
    FROM missing_hospitals m
    LEFT JOIN raw_staging.stg_hospitals s
        ON m.hospital_id = s.facility_id
    LEFT JOIN raw_marts.fct_inpatient_charges c
        ON m.hospital_id = c.hospital_id
        AND c.hospital_key IS NULL
    GROUP BY m.hospital_id, s.facility_id, s.facility_name, s.state
    ORDER BY charge_records DESC
    LIMIT 30
    """
    
    print("Checking if missing hospital_ids exist in stg_hospitals...")
    print()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            rows = result.fetchall()
            
            if rows:
                print(f"{'hospital_id':<12} {'Source Status':<35} {'Facility Name':<40} {'State':<6} {'Records':<10}")
                print("-" * 110)
                
                exists_count = 0
                missing_count = 0
                
                for row in rows:
                    hospital_id = str(row[0])
                    status = row[1]
                    facility_name = row[2] or 'N/A'
                    state = row[3] or 'N/A'
                    records = row[4]
                    
                    if 'EXISTS' in status:
                        exists_count += 1
                    else:
                        missing_count += 1
                    
                    print(f"{hospital_id:<12} {status:<35} {facility_name[:38]:<40} {state:<6} {records:<10}")
                
                print()
                print("=" * 60)
                print("Summary")
                print("=" * 60)
                print(f"Hospitals that EXIST in stg_hospitals: {exists_count}")
                print(f"Hospitals MISSING from stg_hospitals: {missing_count}")
                print()
                
                if exists_count > 0:
                    print("⚠️  ISSUE FOUND:")
                    print("   Some hospitals exist in stg_hospitals but not in dim_hospitals!")
                    print("   This suggests they were filtered out during dim_hospitals creation.")
                    print()
                    print("   Possible causes:")
                    print("   1. Data quality filters in dim_hospitals")
                    print("   2. SCD Type 2 logic filtering them out")
                    print("   3. is_current = TRUE filter excluding them")
                
                if missing_count > 0:
                    print("NOTE:")
                    print("   Some hospitals don't exist in source data at all.")
                    print("   These are orphaned records - charges without hospital info.")
            else:
                print("No missing hospitals found!")
                
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

