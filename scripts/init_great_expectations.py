#!/usr/bin/env python3
"""
Initialize Great Expectations project
Works around Windows PATH issues
"""

import os
import sys
from pathlib import Path

try:
    import great_expectations as gx
except ImportError:
    print("❌ Great Expectations not installed. Run: pip install great-expectations")
    sys.exit(1)

def main():
    """Initialize Great Expectations project"""
    project_root = Path(__file__).parent.parent
    
    print("🚀 Initializing Great Expectations project...")
    print(f"📁 Project root: {project_root}")
    print()
    
    # Check if already initialized
    gx_dir = project_root / "great_expectations"
    if gx_dir.exists():
        print("⚠️  Great Expectations already initialized!")
        print(f"   Directory exists: {gx_dir}")
        response = input("   Do you want to reinitialize? (y/N): ")
        if response.lower() != 'y':
            print("   Skipping initialization.")
            return
    
    try:
        # Change to project root
        os.chdir(project_root)
        
        # Initialize using Python API
        print("📦 Creating Great Expectations context...")
        context = gx.get_context(mode="file")
        
        print("✅ Great Expectations initialized successfully!")
        print()
        print("📝 Next steps:")
        print("   1. Configure datasource:")
        print("      python scripts/configure_gx_datasource.py")
        print()
        print("   2. Create expectation suite:")
        print("      Use GX Data Assistant or create manually")
        print()
        print("   3. Create checkpoint:")
        print("      Use GX CLI or Python API")
        print()
        
    except Exception as e:
        print(f"❌ Error initializing Great Expectations: {e}")
        print()
        print("💡 Try running manually:")
        print("   python -c \"import great_expectations as gx; gx.get_context()\"")
        sys.exit(1)

if __name__ == "__main__":
    main()

