import os
os.environ['DATASET_TYPE'] = 'DATA_MART'
os.environ['DRY_RUN'] = 'true'

from app.load.dm_plan_requests_loader import execute

result = execute()
print(result)