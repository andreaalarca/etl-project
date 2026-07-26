import os
from pathlib import Path
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

from app.pull.base_extractor import BaseExtractor
from app.utils.logger import logger


class ConstructionPlanTypesExtractor(BaseExtractor):
    file_name = "construction_plan_types.csv"

    def __init__(self):
        super().__init__()
        self.staging_dir = Path(os.getenv('STAGING_DIR', './data/staging'))

    def copy_to_raw(self):
        return super().copy_to_raw(self.source_dir / self.file_name, self.raw_dir)

    def extract(self, chunksize=None):
        return super().extract(chunksize=chunksize)


def extract_construction_plan_types(chunksize=None):
    """
    Convenience function that extracts construction_plan_types data using default raw directory.
    """
    extractor = ConstructionPlanTypesExtractor()
    extractor.copy_to_raw()
    return extractor.extract(chunksize=chunksize)


def execute() -> str:
    """
    Execute the construction_plan_types data extraction process.
    This method orchestrates the extract step:
    1. Copy source file to raw directory
    2. Extract data from CSV (handling chunked/non-chunked internally)
    3. Write extracted data to staging area as Parquet for preprocessing

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("Construction Plan Types Extract", "Start", "Beginning construction_plan_types data extraction")

        # Step 1: Copy source file to raw directory
        extractor = ConstructionPlanTypesExtractor()
        raw_file_path = extractor.copy_to_raw()
        logger.info("Construction Plan Types Extract", "Copy Complete", f"Copied source file to: {raw_file_path}")

        # Step 2: Extract data from CSV
        data = extractor.extract()

        # Step 3: Write to staging area for preprocessing
        today = datetime.now().strftime("%Y%m%d")
        staging_file = extractor.staging_dir / f"construction_plan_types_{today}.parquet"

        # Handle both DataFrame and iterator of DataFrames
        if isinstance(data, pd.DataFrame):
            # Single DataFrame case
            data.to_parquet(staging_file, index=False)
            record_count = len(data)
            logger.info("Construction Plan Types Extract", "Write Complete",
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
                logger.info("Construction Plan Types Extract", "Write Complete",
                           f"Wrote {record_count} records from {len(chunks)} chunks to staging file: {staging_file}")
            else:
                # No data case
                df = pd.DataFrame()
                df.to_parquet(staging_file, index=False)
                record_count = 0
                logger.info("Construction Plan Types Extract", "Write Complete",
                           f"No data found, created empty staging file: {staging_file}")

        logger.info("Construction Plan Types Extract", "Success",
                   f"Construction_plan_types extraction completed successfully. Processed {record_count} records.")
        return f"SUCCESS: Extracted {record_count} construction_plan_types records to {staging_file}"

    except Exception as e:
        logger.error("Construction Plan Types Extract", "Error", f"Construction_plan_types extraction failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed