"""
Extractor for plan requests CSV data.
"""

import os
from pathlib import Path
import shutil
from datetime import datetime
import pandas as pd

from .base_extractor import BaseExtractor
from app.utils.logger import logger


class PlanRequestsExtractor(BaseExtractor):
    """
    Extracts plan requests data from CSV.
    """
    file_name = "plan_requests.csv"

    def __init__(self):
        super().__init__()
        self.staging_dir = Path(os.getenv('STAGING_DIR', './data/staging'))

    def copy_to_raw(self):
        """
        Copy the source file to the raw directory.

        Returns:
            Path: The path to the copied file in the raw directory
        """
        return super().copy_to_raw(self.source_dir / self.file_name, self.raw_dir)

    def extract(self, chunksize: int = None):
        """
        Extract plan requests data.

        Args:
            chunksize: If specified, return iterator of DataFrames of given size; else return full DataFrame

        Returns:
            pandas DataFrame or iterator of DataFrames
        """
        return super().extract(chunksize=chunksize)

    @classmethod
    def extract_classmethod(cls, chunksize: int = None):
        """
        Class method shortcut for extraction.

        Args:
            chunksize: If specified, return iterator of DataFrames; else return full DataFrame

        Returns:
            pandas DataFrame or iterator of DataFrames
        """
        extractor = cls()
        extractor.copy_to_raw()
        return extractor.extract(chunksize=chunksize)


def extract_plan_requests(chunksize: int = None):
    """
    Convenience function that extracts plan requests data using default raw directory.

    Args:
        chunksize: If specified, return iterator of DataFrames; else return full DataFrame

    Returns:
        pandas DataFrame or iterator of DataFrames
    """
    extractor = PlanRequestsExtractor()
    extractor.copy_to_raw()
    return extractor.extract(chunksize=chunksize)


def execute() -> str:
    """
    Execute the plan requests data extraction process.
    This method orchestrates the extract step:
    1. Copy source file to raw directory
    2. Extract data from CSV (handling chunked/non-chunked internally)
    3. Write extracted data to staging area as Parquet for preprocessing

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("PlanRequests Extract", "Start", "Beginning plan requests data extraction")

        # Step 1: Copy source file to raw directory
        extractor = PlanRequestsExtractor()
        raw_file_path = extractor.copy_to_raw()
        logger.info("PlanRequests Extract", "Copy Complete", f"Copied source file to: {raw_file_path}")

        # Step 2: Extract data from CSV
        data = extractor.extract()

        # Step 3: Write to staging area for preprocessing
        today = datetime.now().strftime("%Y%m%d")
        staging_file = extractor.staging_dir / f"plan_requests_{today}.parquet"

        # Handle both DataFrame and iterator of DataFrames
        if isinstance(data, pd.DataFrame):
            # Single DataFrame case
            data.to_parquet(staging_file, index=False)
            record_count = len(data)
            logger.info("PlanRequests Extract", "Write Complete",
                       f"Wrote {record_count} records to staging file: {staging_file}")
        else:
            # Iterator of DataFrames case (chunked processing)
            chunks = []
            for chunk in data:
                chunks.append(chunk)

            if chunks:
                df = pd.concat(chunks, ignore_index=True)
                df.to_parquet(staging_file, index=False)
                record_count = len(df)
                logger.info("PlanRequests Extract", "Write Complete",
                           f"Wrote {record_count} records from {len(chunks)} chunks to staging file: {staging_file}")
            else:
                # No data case
                df = pd.DataFrame()
                df.to_parquet(staging_file, index=False)
                record_count = 0
                logger.info("PlanRequests Extract", "Write Complete",
                           f"No data found, created empty staging file: {staging_file}")

        logger.info("PlanRequests Extract", "Success",
                   f"Plan requests extraction completed successfully. Processed {record_count} records.")
        return f"SUCCESS: Extracted {record_count} plan requests records to {staging_file}"

    except Exception as e:
        logger.error("PlanRequests Extract", "Error", f"Plan requests extraction failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed