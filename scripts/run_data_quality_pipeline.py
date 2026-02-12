#!/usr/bin/env python3
"""
Data Quality Pipeline Runner
Runs dbt tests and Great Expectations validations in sequence
"""

import subprocess
import sys
import os
from pathlib import Path

def run_command(command, description):
    """Run a shell command and handle errors"""
    print(f"\n{'='*60}")
    print(f"📊 {description}")
    print(f"{'='*60}")
    print(f"Running: {command}\n")
    
    result = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"❌ Error in {description}")
        print(result.stderr)
        return False
    else:
        print(f"✅ {description} completed successfully")
        if result.stdout:
            print(result.stdout)
        return True

def main():
    """Main pipeline execution"""
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    
    print("🚀 Starting Data Quality Pipeline")
    print("="*60)
    
    # Step 1: Run dbt models
    if not run_command("dbt run", "Building dbt models"):
        print("\n❌ Pipeline failed at dbt run")
        sys.exit(1)
    
    # Step 2: Run dbt tests
    if not run_command("dbt test", "Running dbt tests"):
        print("\n⚠️  Some dbt tests failed, but continuing...")
        # Don't exit - GX might catch additional issues
    
    # Step 3: Run Great Expectations (if configured)
    gx_dir = project_root / "great_expectations"
    if gx_dir.exists():
        # Try to run GX checkpoints
        checkpoints = [
            "marts_suite",
            "staging_suite",
            "full_pipeline"
        ]
        
        for checkpoint in checkpoints:
            cmd = f"great_expectations checkpoint run {checkpoint}"
            if not run_command(cmd, f"Running GX checkpoint: {checkpoint}"):
                print(f"\n⚠️  GX checkpoint {checkpoint} had issues")
    else:
        print("\nℹ️  Great Expectations not configured yet")
        print("   Run: scripts/setup_great_expectations.sh to set it up")
    
    # Step 4: Generate documentation
    if not run_command("dbt docs generate", "Generating dbt documentation"):
        print("\n⚠️  Documentation generation had issues")
    
    # Step 5: Build GX data docs (if configured)
    if gx_dir.exists():
        if not run_command("great_expectations docs build", "Building GX data docs"):
            print("\n⚠️  GX docs generation had issues")
    
    print("\n" + "="*60)
    print("✅ Data Quality Pipeline Complete!")
    print("="*60)
    print("\n📚 View documentation:")
    print("   - dbt docs: dbt docs serve")
    print("   - GX docs: great_expectations docs build --view")

if __name__ == "__main__":
    main()

