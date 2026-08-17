"""
Data Mart model for dm_plan_requests.
Responsible for executing the model SQL and writing staging Parquet file.
"""
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text

from app.config.warehouse_config import WarehouseConfig
from app.utils.logger import logger
from app.utils.staging import get_staging_file_path
from app.utils.process_date import get_process_date


def execute(process_date: str = None) -> dict:
    """
    Execute the dm_plan_requests model transformation.

    Args:
        process_date: Process date in YYYY-MM-DD format. If None, defaults to yesterday.

    Returns:
        dict with keys:
            - staging_file_path: str, path to the written Parquet file
            - row_count: int, number of rows written
            - process_date: str, the process date used (YYYY-MM-DD)
    """
    # Determine process date
    process_date = get_process_date(process_date)
    logger.info("DM Plan Requests Model", "Start", f"Beginning model execution for process date {process_date}")

    # Initialize database connection
    warehouse_config = WarehouseConfig()
    warehouse_url = warehouse_config.warehouse_url
    # Mask password in URL for logging
    masked_url = warehouse_url.replace(
        warehouse_url.split("://")[1].split("@")[0],
        "***:***"
    ) if "@" in warehouse_url else warehouse_url
    logger.info("DM Plan Requests Model", "Database Connection", f"Connecting to warehouse: {masked_url}")
    engine = create_engine(warehouse_url, future=True)

    # Read the model SQL file
    sql_file_path = Path(__file__).parent / "dm_plan_requests.sql"
    with open(sql_file_path, 'r') as f:
        sql_template = f.read()

    # Apply process date to the SQL
    sql_query = sql_template.replace("{{ process_date }}", process_date)
    logger.info("DM Plan Requests Model", "SQL Prepared", f"Executing SQL: {sql_query}")
    logger.info("DM Plan Requests Model", "SQL Prepared", f"Executing SQL for date {process_date}")

    # Execute the SQL and read into DataFrame
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(sql_query), conn)
        logger.info("DM Plan Requests Model", "SQL Execution", f"Retrieved {len(df)} rows from warehouse")
        if len(df) == 0:
            logger.warning("DM Plan Requests Model", "SQL Execution", "Query returned zero rows - check source data for date {process_date}")
    except Exception as e:
        logger.error("DM Plan Requests Model", "SQL Execution", f"Failed to execute model SQL: {str(e)}")
        raise

    # Write DataFrame to staging Parquet file
    staging_file_path = get_staging_file_path("dm_plan_requests", process_date)
    # Ensure staging directory exists
    staging_file_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(staging_file_path, index=False)
    logger.info("DM Plan Requests Model", "Staging Write", f"Wrote {len(df)} rows to {staging_file_path}")

    # Return information for downstream tasks
    return {
        "staging_file_path": str(staging_file_path),
        "row_count": len(df),
        "process_date": process_date
    }