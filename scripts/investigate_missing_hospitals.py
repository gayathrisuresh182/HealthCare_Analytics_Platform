#!/usr/bin/env python3
"""
Investigate which hospital_id values don't match dim_hospitals
"""

import sys
try:
    import great_expectations as gx
    from great_expectations.datasource.fluent import SnowflakeDatasource
except ImportError:
    print("ERROR: Great Expectations not installed.")
    sys.exit(1)

def main():
    context = gx.get_context()
    
    print("=" * 60)
    print("Investigate Missing Hospitals")
    print("=" * 60)
    print()
    
    # Get datasource
    datasource = context.fluent_datasources["snowflake_datasource"]
    
    print("Querying Snowflake to find NULL hospital_key values...")
    print()
    
    # Query 1: Count NULLs
    query1 = """
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(hospital_key) AS rows_with_hospital_key,
        COUNT(*) - COUNT(hospital_key) AS rows_with_null_hospital_key,
        ROUND((COUNT(*) - COUNT(hospital_key)) * 100.0 / COUNT(*), 2) AS pct_null
    FROM raw_marts.fct_inpatient_charges
    """
    
    # Query 2: Sample rows with NULL hospital_key
    query2 = """
    SELECT 
        hospital_id,
        drg_code,
        charge_key,
        total_discharges,
        avg_covered_charges
    FROM raw_marts.fct_inpatient_charges
    WHERE hospital_key IS NULL
    LIMIT 10
    """
    
    # Query 3: Check if hospital_ids exist in dim_hospitals
    query3 = """
    SELECT DISTINCT
        c.hospital_id,
        CASE 
            WHEN h.facility_id IS NOT NULL THEN 'EXISTS in dim_hospitals'
            ELSE 'MISSING from dim_hospitals'
        END AS status,
        COUNT(*) AS charge_record_count
    FROM raw_marts.fct_inpatient_charges c
    LEFT JOIN raw_marts.dim_hospitals h
        ON c.hospital_id = h.facility_id
        AND h.is_current = TRUE
    WHERE c.hospital_key IS NULL
    GROUP BY c.hospital_id, h.facility_id
    ORDER BY charge_record_count DESC
    LIMIT 20
    """
    
    # Query 4: Check all unique hospital_ids in charges vs dim_hospitals
    query4 = """
    SELECT 
        'Charges table' AS source,
        COUNT(DISTINCT hospital_id) AS unique_hospital_ids
    FROM raw_marts.fct_inpatient_charges
    UNION ALL
    SELECT 
        'dim_hospitals (current)' AS source,
        COUNT(DISTINCT facility_id) AS unique_hospital_ids
    FROM raw_marts.dim_hospitals
    WHERE is_current = TRUE
    UNION ALL
    SELECT 
        'Charges with NULL hospital_key' AS source,
        COUNT(DISTINCT hospital_id) AS unique_hospital_ids
    FROM raw_marts.fct_inpatient_charges
    WHERE hospital_key IS NULL
    """
    
    try:
        # Read credentials from dbt profiles (same as datasource config)
        import yaml
        from pathlib import Path
        
        profiles_path = Path.home() / ".dbt" / "profiles.yml"
        if not profiles_path.exists():
            raise Exception("dbt profiles.yml not found")
        
        with open(profiles_path, 'r') as f:
            profiles = yaml.safe_load(f)
        
        dev = profiles['healthcare_analytics']['outputs'].get('dev', {})
        conn_details = {
            'account': dev.get('account', ''),
            'user': dev.get('user', ''),
            'password': dev.get('password', ''),
            'database': dev.get('database', 'HEALTHCARE_ANALYTICS'),
            'warehouse': dev.get('warehouse', 'transforming_wh'),
            'role': dev.get('role', 'ACCOUNTADMIN')
        }
        
        # Execute queries using datasource connection
        from snowflake.sqlalchemy import URL
        from sqlalchemy import create_engine, text
        
        connection_string = f"snowflake://{conn_details['user']}:{conn_details['password']}@{conn_details['account']}/{conn_details['database']}?warehouse={conn_details['warehouse']}&role={conn_details['role']}"
        
        engine = create_engine(connection_string)
        
        print("=" * 60)
        print("1. NULL hospital_key Statistics")
        print("=" * 60)
        with engine.connect() as conn:
            result = conn.execute(text(query1))
            row = result.fetchone()
            print(f"Total rows: {row[0]:,}")
            print(f"Rows with hospital_key: {row[1]:,}")
            print(f"Rows with NULL hospital_key: {row[2]:,}")
            print(f"Percentage NULL: {row[3]}%")
        
        print()
        print("=" * 60)
        print("2. Sample Rows with NULL hospital_key")
        print("=" * 60)
        with engine.connect() as conn:
            result = conn.execute(text(query2))
            rows = result.fetchall()
            if rows:
                print(f"{'hospital_id':<15} {'drg_code':<10} {'total_discharges':<15} {'avg_covered_charges':<20}")
                print("-" * 60)
                for row in rows:
                    print(f"{str(row[0]):<15} {str(row[1]):<10} {str(row[3]):<15} ${row[4]:,.2f}")
            else:
                print("No NULL hospital_key values found!")
        
        print()
        print("=" * 60)
        print("3. Hospital IDs Missing from dim_hospitals")
        print("=" * 60)
        with engine.connect() as conn:
            result = conn.execute(text(query3))
            rows = result.fetchall()
            if rows:
                print(f"{'hospital_id':<15} {'Status':<30} {'Charge Records':<15}")
                print("-" * 60)
                for row in rows:
                    print(f"{str(row[0]):<15} {row[1]:<30} {row[2]:,}")
            else:
                print("All hospital_ids exist in dim_hospitals!")
        
        print()
        print("=" * 60)
        print("4. Hospital ID Counts Comparison")
        print("=" * 60)
        with engine.connect() as conn:
            result = conn.execute(text(query4))
            rows = result.fetchall()
            print(f"{'Source':<35} {'Unique Hospital IDs':<20}")
            print("-" * 55)
            for row in rows:
                print(f"{row[0]:<35} {row[1]:,}")
        
        print()
        print("=" * 60)
        print("Analysis Complete")
        print("=" * 60)
        print()
        print("Next steps:")
        print("  1. Review which hospital_ids are missing")
        print("  2. Check if they exist in source data (stg_hospitals)")
        print("  3. Add missing hospitals to dim_hospitals if they should exist")
        print("  4. Or adjust expectation to allow NULLs if orphaned records are acceptable")
        
    except Exception as e:
        print(f"ERROR: Failed to query Snowflake: {e}")
        import traceback
        traceback.print_exc()
        print()
        print("Alternative: Run the SQL queries manually in Snowflake:")
        print("  See: scripts/check_hospital_key_nulls.sql")
        sys.exit(1)

if __name__ == "__main__":
    main()

