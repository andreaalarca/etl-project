import os
os.environ['DATASET_TYPE'] = 'DATA_MART'
os.environ['DRY_RUN'] = 'true'

from app.model.dm_plan_requests_model import execute as model_execute
from app.load.dm_plan_requests_loader import execute as load_execute

print("=== Testing Complete DM Plan Requests ETL Pipeline ===")
print(f"DATASET_TYPE: {os.environ.get('DATASET_TYPE')}")
print(f"DRY_RUN: {os.environ.get('DRY_RUN')}")
print()

# Step 1: Run the model
print("Step 1: Running Data Mart Model (JOIN operation)")
model_result = model_execute()
print(f"Model Result: {model_result}")
print()

# Step 2: Run the load
print("Step 2: Running Data Mart Load (Persist to data_mart)")
load_result = load_execute()
print(f"Load Result: {load_result}")
print()

# Check if both succeeded
if "SUCCESS" in model_result and "SUCCESS" in load_result:
    print("✅ COMPLETE ETL PIPELINE TEST PASSED")
else:
    print("❌ COMPLETE ETL PIPELINE TEST FAILED")
    if "SUCCESS" not in model_result:
        print(f"  Model failed: {model_result}")
    if "SUCCESS" not in load_result:
        print(f"  Load failed: {load_result}")