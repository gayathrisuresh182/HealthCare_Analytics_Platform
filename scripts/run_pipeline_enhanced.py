#!/usr/bin/env python3
"""
Enhanced Data Pipeline Runner
Runs dbt models, tests, and Great Expectations with error handling and notifications
"""

import subprocess
import sys
import os
import json
import datetime
from pathlib import Path
from typing import Dict, List, Tuple

def run_command(command: str, description: str, continue_on_error: bool = False) -> Tuple[bool, str]:
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
        if not continue_on_error:
            return False, result.stderr
        else:
            print(f"⚠️  Continuing despite error...")
            return True, result.stderr
    else:
        print(f"✅ {description} completed successfully")
        if result.stdout:
            print(result.stdout)
        return True, result.stdout

def load_dbt_results(results_path: Path) -> Dict:
    """Load dbt run results"""
    if results_path.exists():
        with open(results_path, 'r') as f:
            return json.load(f)
    return {}

def get_test_summary(results: Dict) -> Dict:
    """Extract test summary from dbt results"""
    if not results or 'results' not in results:
        return {'passed': 0, 'failed': 0, 'warned': 0}
    
    passed = sum(1 for r in results['results'] if r.get('status') == 'pass')
    failed = sum(1 for r in results['results'] if r.get('status') == 'fail')
    warned = sum(1 for r in results['results'] if r.get('status') == 'warn')
    
    return {'passed': passed, 'failed': failed, 'warned': warned}

def send_notification(status: str, message: str, webhook_url: str = None):
    """Send notification (Slack, email, etc.)"""
    if webhook_url:
        try:
            import requests
            payload = {
                "text": f"🚀 Pipeline {status}: {message}",
                "username": "Healthcare Analytics Pipeline",
                "icon_emoji": ":hospital:" if status == "success" else ":warning:"
            }
            requests.post(webhook_url, json=payload, timeout=10)
        except Exception as e:
            print(f"⚠️  Failed to send notification: {e}")

def main():
    """Main pipeline execution"""
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    
    print("🚀 Starting Enhanced Data Quality Pipeline")
    print("="*60)
    print(f"Timestamp: {datetime.datetime.now().isoformat()}")
    print("="*60)
    
    pipeline_status = "success"
    errors = []
    warnings = []
    
    # Step 1: Install dependencies
    success, output = run_command("dbt deps", "Installing dbt packages", continue_on_error=True)
    if not success:
        warnings.append("dbt deps had issues")
    
    # Step 2: Run dbt models (staging)
    success, output = run_command("dbt run --select staging", "Building staging models")
    if not success:
        errors.append("Staging models failed")
        pipeline_status = "failed"
        print("\n❌ Pipeline failed at staging models")
        sys.exit(1)
    
    # Step 3: Run dbt models (intermediate)
    success, output = run_command("dbt run --select intermediate", "Building intermediate models")
    if not success:
        errors.append("Intermediate models failed")
        pipeline_status = "failed"
        print("\n❌ Pipeline failed at intermediate models")
        sys.exit(1)
    
    # Step 4: Run dbt models (marts)
    success, output = run_command("dbt run --select marts", "Building marts models")
    if not success:
        errors.append("Marts models failed")
        pipeline_status = "failed"
        print("\n❌ Pipeline failed at marts models")
        sys.exit(1)
    
    # Step 5: Run dbt tests
    success, output = run_command("dbt test", "Running dbt tests", continue_on_error=True)
    if not success:
        warnings.append("Some dbt tests failed")
    
    # Load test results
    test_results_path = project_root / "target" / "run_results.json"
    test_results = load_dbt_results(test_results_path)
    test_summary = get_test_summary(test_results)
    
    print(f"\n📊 Test Summary:")
    print(f"   ✅ Passed: {test_summary['passed']}")
    print(f"   ❌ Failed: {test_summary['failed']}")
    print(f"   ⚠️  Warned: {test_summary['warned']}")
    
    if test_summary['failed'] > 0:
        warnings.append(f"{test_summary['failed']} tests failed")
    
    # Step 6: Run Great Expectations (if configured)
    gx_dir = project_root / "gx"
    if gx_dir.exists():
        success, output = run_command(
            "python scripts/gx_run_checkpoint.py",
            "Running Great Expectations validation",
            continue_on_error=True
        )
        if not success:
            warnings.append("Great Expectations validation had issues")
    else:
        print("\nℹ️  Great Expectations not configured (skipping)")
    
    # Step 7: Generate documentation
    success, output = run_command("dbt docs generate", "Generating dbt documentation", continue_on_error=True)
    if not success:
        warnings.append("Documentation generation had issues")
    
    # Step 8: Build GX data docs (if configured)
    if gx_dir.exists():
        success, output = run_command(
            "python scripts/gx_docs_build.py",
            "Building Great Expectations data docs",
            continue_on_error=True
        )
        if not success:
            warnings.append("GX docs generation had issues")
    
    # Final summary
    print("\n" + "="*60)
    if pipeline_status == "success" and len(errors) == 0:
        print("✅ Pipeline Completed Successfully!")
        if warnings:
            print(f"⚠️  Warnings: {len(warnings)}")
            for warning in warnings:
                print(f"   - {warning}")
    else:
        print("❌ Pipeline Completed with Errors!")
        print(f"   Errors: {len(errors)}")
        for error in errors:
            print(f"   - {error}")
        pipeline_status = "failed"
    
    print("="*60)
    print(f"\n📚 View documentation:")
    print("   - dbt docs: dbt docs serve")
    print("   - GX docs: python scripts/gx_docs_build.py")
    
    # Send notification (if webhook URL is set)
    webhook_url = os.getenv('SLACK_WEBHOOK_URL')
    if webhook_url:
        message = f"Pipeline completed. Tests: {test_summary['passed']} passed, {test_summary['failed']} failed"
        send_notification(pipeline_status, message, webhook_url)
    
    # Exit with appropriate code
    if pipeline_status == "failed":
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()

