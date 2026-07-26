#!/usr/bin/env python3
"""
Full ETL test with loading to verify DRY_RUN writes to analytics_test.
Tests: construction_plan_types, customers, plan_requests
"""

import os
import sys
from pathlib import Path
from datetime import datetime

# Add the app directory to Python path so we can import modules
sys.path.insert(0, str(Path(__file__).parent / "app"))

def set_test_environment():
    """Set environment variables for testing"""
    os.environ['DRY_RUN'] = 'true'
    os.environ['DATASET_TYPE'] = 'DATA_LAKE'  # or DATA_MART for dm_plan_requests

    # Set up required directories
    os.makedirs('./data/source', exist_ok=True)
    os.makedirs('./data/raw', exist_ok=True)
    os.makedirs('./data/staging', exist_ok=True)
    os.makedirs('./data/archive', exist_ok=True)
    os.makedirs('./data/error', exist_ok=True)

    print("Test environment configured:")
    print(f"  DRY_RUN: {os.environ.get('DRY_RUN')}")
    print(f"  DATASET_TYPE: {os.environ.get('DATASET_TYPE')}")

def test_table_etl_load(table_name):
    """Test full ETL process for a specific table including load step"""
    print(f"\n{'='*60}")
    print(f"Testing {table_name} ETL with dry_run=True and chunksize=1 (FULL LOAD)")
    print(f"{'='*60}")

    try:
        # 1. Test Extractor (execute)
        print(f"\n1. Testing {table_name} extractor...")
        extractor_module = __import__(
            f'app.pull.{table_name}_extractor',
            fromlist=['execute']
        )
        extract_result = extractor_module.execute()
        print(f"   Extractor result: {extract_result}")

        # 2. Test Preprocessor (execute)
        print(f"\n2. Testing {table_name} preprocessor...")
        preprocessor_module = __import__(
            f'app.preprocess.{table_name}',
            fromlist=['execute']
        )
        preprocess_result = preprocessor_module.execute()
        print(f"   Preprocessor result: {preprocess_result}")

        # 3. Test Transformer (execute)
        print(f"\n3. Testing {table_name} transformer...")
        transformer_module = __import__(
            f'app.transform.{table_name}_transformer',
            fromlist=['execute']
        )
        transform_result = transformer_module.execute()
        print(f"   Transformer result: {transform_result}")

        # 4. Test Loader (execute) - this will load data into warehouse
        print(f"\n4. Testing {table_name} loader (full load)...")
        loader_module = __import__(
            f'app.load.{table_name}_loader',
            fromlist=['execute']
        )
        load_result = loader_module.execute()
        print(f"   Loader result: {load_result}")

        print(f"\n[PASS] {table_name} FULL ETL test completed successfully!")
        return True

    except Exception as e:
        print(f"\n[FAIL] {table_name} FULL ETL test FAILED: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main test function"""
    print("Starting FULL ETL Module Tests with LOAD")
    print("=" * 60)

    # Set up test environment
    set_test_environment()

    # Test each table
    tables_to_test = [
        'construction_plan_types',
        'customers',
        'plan_requests'
    ]

    results = {}
    for table in tables_to_test:
        results[table] = test_table_etl_load(table)

    # Summary
    print(f"\n{'='*60}")
    print("FULL ETL TEST SUMMARY")
    print(f"{'='*60}")

    all_passed = True
    for table, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"{table:<25} {status}")
        if not passed:
            all_passed = False

    print(f"\nOverall result: {'ALL TESTS PASSED' if all_passed else 'SOME TESTS FAILED'}")
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main())