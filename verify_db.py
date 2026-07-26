import os
os.environ['DATASET_TYPE'] = 'DATA_MART'
os.environ['DRY_RUN'] = 'true'

from sqlalchemy import create_engine, text
from app.config.warehouse_config import WarehouseConfig

# Test direct database access to verify data was loaded
try:
    warehouse_config = WarehouseConfig()
    engine = create_engine(warehouse_config.warehouse_url, future=True)

    with engine.connect() as conn:
        # Check how many records are in the dm_plan_requests table
        result = conn.execute(text('SELECT COUNT(*) FROM data_mart.dm_plan_requests'))
        count = result.scalar()
        print(f"Number of records in data_mart.dm_plan_requests: {count}")

        # Show a sample of the data
        result = conn.execute(text('''
            SELECT request_id, customer_name, plan_type, base_price
            FROM data_mart.dm_plan_requests
            LIMIT 3
        '''))
        rows = result.fetchall()
        print("\nSample data:")
        for row in rows:
            print(f"  Request {row[0]}: {row[1]} - {row[2]} - ${float(row[3]) if row[3] else 0:.2f}")

except Exception as e:
    print(f"Error accessing database: {e}")
    import traceback
    traceback.print_exc()