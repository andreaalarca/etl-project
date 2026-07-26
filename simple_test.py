import os
os.environ['DATASET_TYPE'] = 'DATA_MART'
os.environ['DRY_RUN'] = 'true'

from app.model.dm_plan_requests_model import execute

result = execute()
print(result)