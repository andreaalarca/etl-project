#!/usr/bin/env python3
"""
Test script for DM Plan Requests Data Mart Model
"""

import os
import sys
from pathlib import Path

# Add the app directory to Python path
sys.path.insert(0, str(Path(__file__).parent / "app"))

# Set environment variables for testing
os.environ['DATASET_TYPE'] = 'DATA_MART'
os.environ['DRY_RUN'] = 'true'

# Make sure data directories exist
os.makedirs('./data/source', exist_ok=True)
os.makedirs('./data/raw', exist_ok=True)
os.makedirs('./data/staging', exist_ok=True)
os.makedirs('./data/archive', exist_ok=True)
os.makedirs('./data/error', exist_ok=True)

def test_datamart_model():
    """Test the datamart model component"""
    try:
        # Import and run the model
        from app.model.dm_plan_requests_model import execute

        print("Testing DM Plan Requests Data Mart Model...")
        print(f"DATASET_TYPE: {os.environ.get('DATASET_TYPE')}")
        print(f"DRY_RUN: {os.environ.get('DRY_RUN')}")

        result = execute()
        print(f"Result: {result}")

        if "SUCCESS" in result:
            print("✅ Model test PASSED")
            return True
        else:
            print("❌ Model test FAILED")
            return False

    except Exception as e:
        print(f"❌ Model test ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_datamart_model()
    sys.exit(0 if success else 1)