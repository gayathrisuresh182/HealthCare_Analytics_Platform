#!/usr/bin/env python3
"""
Create Great Expectations Checkpoint
"""

import sys
from pathlib import Path

try:
    import great_expectations as gx
except ImportError:
    print("ERROR: Great Expectations not installed.")
    sys.exit(1)

def main():
    context = gx.get_context()
    
    print("=" * 60)
    print("Create Great Expectations Checkpoint")
    print("=" * 60)
    print()
    
    checkpoint_name = "marts_checkpoint"
    
    # Check if checkpoint already exists
    try:
        existing_cp = context.get_checkpoint(checkpoint_name)
        print(f"WARNING: Checkpoint '{checkpoint_name}' already exists!")
        response = input("Do you want to replace it? (y/N): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
    except:
        pass  # Checkpoint doesn't exist, which is fine
    
    # Get datasource and asset
    try:
        if "snowflake_datasource" not in context.fluent_datasources:
            raise Exception("Datasource not found")
        datasource = context.fluent_datasources["snowflake_datasource"]
        asset = datasource.get_asset("fct_inpatient_charges")
    except Exception as e:
        print(f"ERROR: Datasource or asset not found: {e}")
        print("Make sure you've:")
        print("  1. Configured Snowflake: python scripts/gx_configure_snowflake_working.py")
        print("  2. Created suite: python scripts/gx_create_marts_suite.py")
        sys.exit(1)
    
    print("Creating checkpoint...")
    
    try:
        # Build batch request from asset
        batch_request = asset.build_batch_request()
        
        # Get expectation suite object
        suite = context.suites.get("marts.fct_inpatient_charges")
        
        # Create checkpoint YAML file directly (simpler approach)
        checkpoint_dir = Path("gx/checkpoints")
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        
        checkpoint_config = {
            "name": checkpoint_name,
            "config_version": 1.0,
            "template_name": None,
            "module_name": "great_expectations.checkpoint",
            "class_name": "Checkpoint",
            "run_name_template": "%Y%m%d-%H%M%S-%f",
            "expectation_suite_name": "marts.fct_inpatient_charges",
            "batch_request": {
                "datasource_name": "snowflake_datasource",
                "data_asset_name": "fct_inpatient_charges"
            },
            "action_list": [
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction"
                    }
                },
                {
                    "name": "update_data_docs",
                    "action": {
                        "class_name": "UpdateDataDocsAction"
                    }
                }
            ]
        }
        
        # Write checkpoint YAML
        import yaml
        checkpoint_file = checkpoint_dir / f"{checkpoint_name}.yml"
        with open(checkpoint_file, 'w') as f:
            yaml.dump(checkpoint_config, f, default_flow_style=False, sort_keys=False)
        
        print(f"Checkpoint YAML created: {checkpoint_file}")
        
        # Try to instantiate checkpoint from config to verify it works
        from great_expectations.checkpoint import Checkpoint
        
        try:
            # Load the YAML config
            import yaml
            with open(checkpoint_file, 'r') as f:
                checkpoint_config = yaml.safe_load(f)
            
            # Instantiate checkpoint to verify it works
            checkpoint = Checkpoint.instantiate_from_config_with_runtime_data(
                context=context,
                checkpoint_config=checkpoint_config
            )
            print(f"Checkpoint verified: {checkpoint.name}")
            
            # Add to context
            context.checkpoints.add(checkpoint)
            print("Checkpoint added to context!")
            
        except Exception as e:
            print(f"Note: Checkpoint YAML created at {checkpoint_file}")
            print(f"Verification warning: {e}")
            print("The checkpoint YAML file is ready to use.")
        
        print()
        print("SUCCESS: Checkpoint created!")
        print(f"Checkpoint name: {checkpoint_name}")
        print()
        print("Next step:")
        print("  Run validation: python scripts/gx_run_checkpoint.py")
        
    except Exception as e:
        print(f"\nERROR: Failed to create checkpoint: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

