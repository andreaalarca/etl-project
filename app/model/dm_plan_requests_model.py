import os
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from app.config.warehouse_config import WarehouseConfig
from app.utils.logger import logger

load_dotenv()


class DMPLANREQUESTSTransformer:

    def __init__(self):
        # Initialize database connection
        warehouse_config = WarehouseConfig()
        self.warehouse_url = warehouse_config.warehouse_url
        self.engine = create_engine(self.warehouse_url, future=True)
        logger.info("DM Plan Requests Job", "Transform", "Initialized transformer for data mart join")

    def transform(self, execution_date: str = None) -> pd.DataFrame:
        """
        Transform data by joining plan_requests, customers, and construction_plan_types from data_lake schema.

        Args:
            execution_date: The execution date in YYYY-MM-DD format. If None, uses today's date in Philippines timezone.

        Returns:
            pandas DataFrame with the joined data.
        """
        logger.info("DM Plan Requests Job", "Transform", "Starting data transformation with JOIN")

        # If execution_date is not provided, use today's date in Philippines timezone
        if execution_date is None:
            ph_tz = ZoneInfo("Asia/Manila")
            today = datetime.now(ph_tz)
            execution_date = today.strftime("%Y-%m-%d")
            logger.info("DM Plan Requests Job", "Transform", f"No execution_date provided, using Philippines today: {execution_date}")

        # Validate format (optional)
        # Basic validation: expect YYYY-MM-DD
        # If not matching, we still use as-is but log warning
        if not (len(execution_date) == 10 and execution_date[4] == '-' and execution_date[7] == '-'):
            logger.warning("DM Plan Requests Job", "Transform", f"Unexpected execution_date format: {execution_date}")

        # Base SQL query with WHERE clause
        query = text(f"""
            SELECT
                pr.request_id,
                c.customer_id,
                c.customer_name,
                c.contact_person,
                c.phone_number,
                c.email,
                c.city,
                c.subcontractor_category,

                pt.plan_type_id,
                pt.plan_type,
                pt.plan_category,
                pt.required_input,
                pt.output_format,
                pt.complexity_level,
                pt.base_price,

                pr.request_date,
                pr.target_date,
                pr.completed_date,
                pr.status,
                pr.priority,
                pr.assigned_coordinator,
                pr.floor_area_sqm,
                pr.revision_count

            FROM data_lake.plan_requests pr
            INNER JOIN data_lake.customers c
                ON pr.customer_id = c.customer_id
            INNER JOIN data_lake.construction_plan_types pt
                ON pr.plan_type_id = pt.plan_type_id
            WHERE pr.request_date = DATE '{execution_date}'
        """)

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
                logger.info("DM Plan Requests Job", "Transform", f"Retrieved {len(df)} rows from joined tables")
        except Exception as e:
            logger.error("DM Plan Requests Job", "Transform", f"Failed to execute join query: {str(e)}")
            raise

        if df.empty:
            logger.warning("DM Plan Requests Job", "Transform", "Received empty DataFrame from join")
            return df

        # Apply final data type conversions to match the target table schema
        # ID columns should be integers
        id_columns = ['request_id', 'customer_id', 'plan_type_id']
        for col in id_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # String columns
        string_columns = [
            'customer_name', 'contact_person', 'phone_number', 'email',
            'city', 'subcontractor_category', 'plan_type', 'plan_category',
            'required_input', 'output_format', 'complexity_level',
            'status', 'priority', 'assigned_coordinator'
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype('string')

        # Date columns
        date_columns = ['request_date', 'target_date', 'completed_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Numeric columns
        numeric_columns = ['floor_area_sqm', 'base_price']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float64')

        # Revision count should be integer
        if 'revision_count' in df.columns:
            df['revision_count'] = pd.to_datetime(df['revision_count'], errors='coerce').fillna(0).astype('int64')

        logger.info("DM Plan Requests Job", "Transform", f"Transformation complete. Final shape: {df.shape}")
        return df


def _get_php_today_str() -> str:
    """Return today's date in Philippines timezone as YYYY-MM-DD string."""
    ph_tz = ZoneInfo("Asia/Manila")
    return datetime.now(ph_tz).strftime("%Y-%m-%d")


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
            execution_date = _get_php_today_str()
        staging_dir = Path(os.getenv("STAGING_DIR", "./data/staging"))
        return staging_dir / f"dm_plan_requests_{execution_date}.parquet"

    def execute(self, execution_date: str = None) -> str:
        """
        Execute the data modeling process for dm_plan_requests.

        Args:
            execution_date: The execution date in YYYY-MM-DD format. If None, uses today's date in Philippines timezone.

        Returns:
            str: Status message indicating success and details
        """
        if execution_date is None:
            execution_date = _get_php_today_str()
        logger.info("DM Plan Requests Model", "Execute Start", f"Beginning model execute for date {execution_date}")
        try:
            logger.info("DM Plan Requests Model", "Start", f"Beginning DM plan requests data modeling for {execution_date}")

            # Step 1: Execute the model transformation to get the joined dataset
            logger.info("DM Plan Requests Model", "Transform", "Executing model join transformation")

            # Use the datamart transformer to get the joined data
            transformer = DMPLANREQUESTSTransformer()
            df = transformer.transform(execution_date=execution_date)

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
        execution_date: The execution date in YYYY-MM-DD format. If None, uses today's date in Philippines timezone.

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