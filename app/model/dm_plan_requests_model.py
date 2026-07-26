"""
Model for DM Plan Requests data.
Responsible for joining data from data_lake schema and writing to staging area.
"""

import os
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
    - Writing the modeled dataset to staging area as Parquet
    """

    def __init__(self):
        # Use WarehouseConfig to handle database connection details
        warehouse_config = WarehouseConfig()
        self.warehouse_url = warehouse_config.warehouse_url

        self.batch_size = int(
            os.getenv("BATCH_SIZE", "1000")
        )

        # Set up staging directory for model output
        today = self._get_today_date()
        self.staging_dir = Path(
            os.getenv(
                "STAGING_DIR",
                "./data/staging"
            )
        )
        self.model_output_path = self.staging_dir / f"dm_plan_requests_model_{today}.parquet"

    def _get_today_date(self):
        """Get today's date in YYYYMMDD format."""
        from datetime import datetime
        return datetime.now().strftime("%Y%m%d")

    def execute(self) -> str:
        """
        Execute the data modeling process for dm_plan_requests.

        Returns:
            str: Status message indicating success and details
        """
        try:
            logger.info("DM Plan Requests Model", "Start", "Beginning DM plan requests data modeling")

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
                # Still write empty file to maintain pipeline consistency
                df.to_parquet(self.model_output_path, index=False)
                logger.info("DM Plan Requests Model", "Write Complete",
                           f"Wrote empty model output to {self.model_output_path}")
                return f"SUCCESS: Modeled 0 dm_plan_requests records (empty result)"

            # Step 2: Write the modeled dataset to staging area as Parquet
            logger.info("DM Plan Requests Model", "Write",
                       f"Writing modeled dataset to {self.model_output_path}")
            df.to_parquet(self.model_output_path, index=False)

            logger.info("DM Plan Requests Model", "Write Complete",
                       f"Wrote {len(df)} modeled records to {self.model_output_path}")

            logger.info("DM Plan Requests Model", "Success",
                       f"DM plan requests data modeling completed successfully. Modeled {len(df)} records.")

            return f"SUCCESS: Modeled {len(df)} dm_plan_requests records"

        except Exception as e:
            logger.error("DM Plan Requests Model", "Error",
                        f"DM plan requests data modeling failed: {str(e)}")
            raise  # Re-raise so Airflow marks task as failed


def execute() -> str:
    """
    Execute the dm_plan_requests data modeling process.
    This function orchestrates the model step for Airflow.

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("DM Plan Requests Model", "Start", "Beginning dm_plan_requests data modeling")

        # Import here to avoid circular imports
        from datetime import datetime

        # Step 1: Locate where to write model output (staging area)
        today = datetime.now().strftime("%Y%m%d")
        from pathlib import Path
        import os
        staging_dir = Path(os.getenv('STAGING_DIR', './data/staging'))
        model_output_path = staging_dir / f"dm_plan_requests_model_{today}.parquet"

        # Step 2: Create model instance and run the modeling process
        model = DmPlanRequestsModel()

        # Override the output path to use the daily staging file pattern
        # This matches what the loader expects to read
        model.staging_dir = staging_dir
        model.model_output_path = Path(
            f"data/staging/dm_plan_requests_{today}.parquet"
        )

        # Step 3: Execute the model
        result = model.execute()

        logger.info("DM Plan Requests Model", "Success",
                   f"DM plan requests data modeling completed: {result}")

        return result

    except Exception as e:
        logger.error("DM Plan Requests Model", "Error",
                    f"DM plan requests data modeling failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed