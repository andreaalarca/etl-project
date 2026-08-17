"""
Data Mart materialization for dm_plan_requests.
Responsible for loading validated staging data into the target table.
"""
import os
from pathlib import Path
import pandas as pd

from app.utils.logger import logger
from app.utils.staging import archive_staging_file
from app.utils.process_date import get_process_date
from app.utils.database import execute_delete_and_insert, get_warehouse_engine
from sqlalchemy import text


def execute(staging_file_path: str, process_date: str = None) -> dict:
    """
    Materialize the dm_plan_requests staging data into the target table.

    Args:
        staging_file_path: Path to the validated staging Parquet file
        process_date: Process date in YYYY-MM-DD format. If None, defaults to yesterday.

    Returns:
        dict with keys:
            - staging_file_path: str, same as input (for possible archiving by caller)
            - process_date: str, the process date used (YYYY-MM-DD)
            - row_count: int, number of rows loaded
            - table_name: str, target table name
            - schema: str, target schema

    Raises:
        Exception: If materialization fails (triggers rollback)
        FileNotFoundError: If staging file does not exist
    """
    # Determine process date
    process_date = get_process_date(process_date)
    logger.info("DM Plan Requests Materialization", "Start", f"Beginning materialization for process date {process_date}")

    # Check if file exists
    staging_path = Path(staging_file_path)
    if not staging_path.is_file():
        raise FileNotFoundError(f"Staging file not found: {staging_file_path}")

    # Read the validated staging file
    try:
        df = pd.read_parquet(staging_path)
    except Exception as e:
        raise Exception(f"Failed to read staging Parquet file: {str(e)}")

    row_count = len(df)
    logger.info("DM Plan Requests Materialization", "Read Staging", f"Read {row_count} rows from {staging_file_path}")

    # Define target table and schema
    table_name = "dm_plan_requests"
    schema = "data_mart"
    date_column = "request_date"  # column to use for DELETE

    # Execute DELETE + INSERT in a single transaction
    try:
        execute_delete_and_insert(
            df=df,
            table_name=table_name,
            schema=schema,
            date_column=date_column,
            process_date=process_date,
            batch_size=int(os.getenv("BATCH_SIZE", "1000"))
        )
        logger.info("DM Plan Requests Materialization", "Load Success", f"Successfully loaded {row_count} rows into {schema}.{table_name}")
    except Exception as e:
        logger.error("DM Plan Requests Materialization", "Load Failure", f"Materialization failed: {str(e)}")
        raise  # Re-raise to allow Airflow to mark task as failed

    # Archive the staging file after successful commit
    try:
        archived_path = archive_staging_file(
            source_file=staging_path,
            table_name=table_name,
            archive_base_dir=None  # Uses default from env var or './data/archive'
        )

        logger.info(
            "DM Plan Requests Materialization",
            "Archive Success",
            f"Archived staging file to {archived_path}"
        )

    except Exception as e:
        # Archive failure should not rollback the database transaction (already committed)
        logger.error("DM Plan Requests Materialization", "Archive Failure", f"Failed to archive staging file: {str(e)}")
        # We do not raise here because the database transaction succeeded

    # Return information for possible downstream tasks (though archive is the last step)
    return {
        "staging_file_path": str(staging_path),
        "process_date": process_date,
        "row_count": row_count,
        "table_name": table_name,
        "schema": schema
    }