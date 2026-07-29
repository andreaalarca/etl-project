"""
Model for DM Plan Requests data.
Responsible for joining data from data_lake schema.
"""
import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from app.config.warehouse_config import WarehouseConfig
from app.utils.logger import logger

# Import the actual transformation logic from datamart
from datamart.transform.dm_plan_requests_transform import DMPLANREQUESTSTransformer

load_dotenv()


class DmPlanRequestsModel:
    """
    Model for DM Plan Requests data.

    Responsible for:
    - Connecting to PostgreSQL
    - Reading source tables from data_lake schema
    - Executing SQL joins to create the analytical dataset
    - Writing staged data to Parquet file
    """

    def __init__(self):
        # Use WarehouseConfig to handle database connection details
        warehouse_config = WarehouseConfig()
        self.warehouse_url = warehouse_config.warehouse_url

        self.batch_size = int(os.getenv("BATCH_SIZE", "1000"))

    def _get_staging_file_path(self, execution_date: str = None) -> Path:
        """Get the path for the staged Parquet file."""
        if execution_date is None:
            execution_date = datetime.now().strftime("%Y%m%d")
        staging_dir = Path(os.getenv("STAGING_DIR", "./data/staging"))
        return staging_dir / f"dm_plan_requests_{execution_date}.parquet"

    def execute(self, execution_date: str = None) -> str:
        """
        Execute the data modeling process for dm_plan_requests.

        Args:
            execution_date: The execution date in YYYYMMDD format. If None, uses today's date.

        Returns:
            str: Status message indicating success and details
        """
        if execution_date is None:
            execution_date = datetime.now().strftime("%Y%m%d")
        logger.info("DM Plan Requests Model", "Execute Start", f"Beginning model execute for date {execution_date}")
        try:
            logger.info("DM Plan Requests Model", "Start", f"Beginning DM plan requests data modeling for {execution_date}")

            # Step 1: Execute the model transformation to get the joined dataset
            logger.info("DM Plan Requests Model", "Transform", "Executing model join transformation")

            # Use the datamart transformer to get the joined data
            transformer = DMPLANREQUESTSTransformer()
            df = transformer.transform()

            record_count = len(df)
            logger.info("DM Plan Requests Model", "Transform Complete",
                       f"Retrieved {record_count} rows from joined tables")

            if df.empty:
                logger.warning("DM Plan Requests Model", "Empty Result",
                              "Model transformation returned empty DataFrame")
                # Still write an empty file to maintain pipeline consistency
                staging_file = self._get_staging_file_path(execution_date)
                # Ensure directory exists
                staging_file.parent.mkdir(parents=True, exist_ok=True)
                df.to_parquet(staging_file, index=False)
                return f"SUCCESS: Modeled 0 dm_plan_requests records for {execution_date} (empty dataset)"

            # Step 2: Write DataFrame to staging area as Parquet
            staging_file = self._get_staging_file_path(execution_date)
            # Ensure directory exists
            staging_file.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(staging_file, index=False)

            logger.info("DM Plan Requests Model", "Write Complete",
                       f"Wrote {record_count} records to staging file: {staging_file}")

            logger.info("DM Plan Requests Model", "Success",
                       f"DM plan requests data modeling completed successfully. Modeled {record_count} records for {execution_date}.")

            return f"SUCCESS: Modeled {record_count} dm_plan_requests records to {staging_file.name} for {execution_date}"

        except Exception as e:
            logger.error("DM Plan Requests Model", "Error",
                        f"DM plan requests data modeling failed for {execution_date}: {str(e)}")
            raise  # Re-raise so Airflow marks task as failed


def execute(execution_date: str = None) -> str:
    """
    Execute the dm_plan_requests data modeling process.
    This function orchestrates the model step for Airflow.

    Args:
        execution_date: The execution date in YYYYMMDD format. If None, uses today's date.

    Returns:
        str: Status message from the model execution
    """
    logger.info("DM Plan Requests Model", "Start", f"Beginning dm_plan_requests data modeling for {execution_date or 'today'}")

    # Import here to avoid circular imports
    from datetime import datetime
    import json

    # Step 1: Create model instance and run the modeling process
    model = DmPlanRequestsModel()

    # Step 2: Execute the model to get status message
    result = model.execute(execution_date=execution_date)

    logger.info("DM Plan Requests Model", "Success",
               f"DM plan requests data modeling completed. {result}")

    return result