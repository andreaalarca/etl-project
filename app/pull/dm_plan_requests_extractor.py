"""
Extractor for dm_plan_requests data.
For datamart tables, extraction is handled by the transformer which reads from source tables.
"""

import os
from pathlib import Path
from datetime import datetime
import pandas as pd

from app.utils.logger import logger


def extract_dm_plan_requests(chunksize: int = None):
    """
    Placeholder for dm_plan_requests extraction.
    For datamart tables, data is extracted by the transformer from source tables.

    Args:
        chunksize: If specified, return iterator of DataFrames; else return full DataFrame

    Returns:
        pandas DataFrame or iterator of DataFrames
    """
    # For datamart tables, we return an empty DataFrame as extraction happens in transform
    return pd.DataFrame()


def execute() -> str:
    """
    Execute the dm_plan_requests data extraction process.
    For datamart tables, this is a placeholder as extraction happens in the transform step.

    Returns:
        str: Status message indicating success
    """
    try:
        logger.info("DM Plan Requests Extract", "Start", "Beginning dm_plan_requests data extraction (placeholder)")

        # For datamart tables like dm_plan_requests, the actual data extraction
        # happens in the transform step when we join the source tables from data_lake
        logger.info("DM Plan Requests Extract", "Info", "No extraction needed for datamart tables - data comes from source tables in transform")

        # Create an empty staging file to maintain pipeline consistency
        today = datetime.now().strftime("%Y%m%d")
        staging_dir = Path(os.getenv('STAGING_DIR', './data/staging'))
        staging_file = staging_dir / f"dm_plan_requests_{today}.parquet"

        # Create empty DataFrame and save to staging
        df = pd.DataFrame()
        df.to_parquet(staging_file, index=False)
        logger.info("DM Plan Requests Extract", "Write Complete", f"Created empty staging file: {staging_file}")

        logger.info("DM Plan Requests Extract", "Success", "DM plan requests extraction completed (placeholder)")
        return f"SUCCESS: Placeholder extraction completed for dm_plan_requests"

    except Exception as e:
        logger.error("DM Plan Requests Extract", "Error", f"DM plan requests extraction failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed