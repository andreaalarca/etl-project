"""
Preprocessor for DM Plan Requests data.
For data that comes from database joins (rather than extracted files),
minimal preprocessing is needed as the transformer handles data type conversions.
"""

import pandas as pd
from app.utils.logger import logger


class DmPlanRequestsPreprocessor:
    """
    Pass-through preprocessor for DM Plan Requests data.

    Since this data comes from database joins in the transformer
    (rather than extracted files requiring cleaning), this preprocessor
    primarily exists to maintain the factory pattern and passes data through unchanged.
    """

    def __init__(self):
        """Initialize the preprocessor."""
        logger.info("DM Plan Requests Job", "Preprocess", "Initialized DM Plan Requests preprocessor")

    def process(self, df: pd.DataFrame, writer=None):
        """
        Process the dataframe (pass-through implementation).

        Args:
            df: Input DataFrame (from transformer)
            writer: Unused writer parameter (for interface compatibility)

        Returns:
            Tuple of (processed_dataframe, writer)
        """
        logger.info("DM Plan Requests Job", "Preprocess", f"Processing {len(df)} rows (pass-through)")

        # For DM Plan Requests, the transformer already handles all data type conversions
        # and cleaning needed, so we just pass the data through
        return df, writer


def execute() -> str:
    """
    Execute the dm_plan_requests data preprocessing process.
    This method orchestrates the preprocess step:
    1. Read previously extracted data from staging area
    2. Validate and clean the data (pass-through for dm_plan_requests)
    3. Handle missing values according to strategy (minimal for dm_plan_requests)
    4. Write processed data back to staging area (overwriting input)

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("DM Plan Requests Preprocess", "Start", "Beginning dm_plan_requests data preprocessing")

        # Step 1: Locate input file from extraction phase
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        import os
        from pathlib import Path
        staging_dir = Path(os.getenv('STAGING_DIR', './data/staging'))
        input_file = staging_dir / f"dm_plan_requests_{today}.parquet"

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}. Ensure extraction step has completed.")

        # Step 2: Read data from staging area
        df = pd.read_parquet(input_file)
        initial_count = len(df)
        logger.info("DM Plan Requests Preprocess", "Read Complete", f"Read {initial_count} records from {input_file}")

        if df.empty:
            logger.warning("DM Plan Requests Preprocess", "Empty Input", "Received empty DataFrame from extraction")
            # Still write empty DataFrame to maintain pipeline
            df.to_parquet(input_file, index=False)
            return f"SUCCESS: Preprocessed 0 dm_plan_requests records (empty input)"

        # Step 3: Process data (pass-through for dm_plan_requests)
        preprocessor = DmPlanRequestsPreprocessor()
        processed_df, _ = preprocessor.process(df, None)  # We don't need the writer for single write
        processed_count = len(processed_df)

        # Step 4: Write processed data back to staging area (overwrite input)
        processed_df.to_parquet(input_file, index=False)
        logger.info("DM Plan Requests Preprocess", "Write Complete",
                   f"Wrote {processed_count} processed records to {input_file}")

        # Log filtering info if any rows were removed
        filtered_count = initial_count - processed_count
        if filtered_count > 0:
            logger.info("DM Plan Requests Preprocess", "Filtering Info",
                       f"Filtered out {filtered_count} records during preprocessing")

        logger.info("DM Plan Requests Preprocess", "Success",
                   f"DM plan requests preprocessing completed successfully. Processed {processed_count} records.")
        return f"SUCCESS: Preprocessed {processed_count} dm_plan_requests records"

    except Exception as e:
        logger.error("DM Plan Requests Preprocess", "Error", f"DM plan requests preprocessing failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed


def execute() -> str:
    """
    Execute the dm_plan_requests data preprocessing process.
    This method orchestrates the preprocess step:
    1. Read previously extracted data from staging area
    2. Validate and clean the data (pass-through for dm_plan_requests)
    3. Handle missing values according to strategy (minimal for dm_plan_requests)
    4. Write processed data back to staging area (overwriting input)

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("DM Plan Requests Preprocess", "Start", "Beginning dm_plan_requests data preprocessing")

        # Step 1: Locate input file from extraction phase
        from datetime import datetime
        today = datetime.now().strftime("%Y%m%d")
        input_file = "/c/Users/Lou/OneDrive/Desktop/etl-project/app/utils/logger.py".replace("app/utils/logger.py", f"data/staging/dm_plan_requests_{today}.parquet")
        input_file = __file__.replace("app/preprocess/dm_plan_requests.py", f"data/staging/dm_plan_requests_{today}.parquet")

        # Alternative approach to get the staging directory
        import os
        from pathlib import Path
        staging_dir = Path(os.getenv('STAGING_DIR', './data/staging'))
        input_file = staging_dir / f"dm_plan_requests_{today}.parquet"

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}. Ensure extraction step has completed.")

        # Step 2: Read data from staging area
        df = pd.read_parquet(input_file)
        initial_count = len(df)
        logger.info("DM Plan Requests Preprocess", "Read Complete", f"Read {initial_count} records from {input_file}")

        if df.empty:
            logger.warning("DM Plan Requests Preprocess", "Empty Input", "Received empty DataFrame from extraction")
            # Still write empty DataFrame to maintain pipeline
            df.to_parquet(input_file, index=False)
            return f"SUCCESS: Preprocessed 0 dm_plan_requests records (empty input)"

        # Step 3: Process data (pass-through for dm_plan_requests)
        preprocessor = DmPlanRequestsPreprocessor()
        processed_df, _ = preprocessor.process(df, None)  # We don't need the writer for single write
        processed_count = len(processed_df)

        # Step 4: Write processed data back to staging area (overwrite input)
        processed_df.to_parquet(input_file, index=False)
        logger.info("DM Plan Requests Preprocess", "Write Complete",
                   f"Wrote {processed_count} processed records to {input_file}")

        # Log filtering info if any rows were removed
        filtered_count = initial_count - processed_count
        if filtered_count > 0:
            logger.info("DM Plan Requests Preprocess", "Filtering Info",
                       f"Filtered out {filtered_count} records during preprocessing")

        logger.info("DM Plan Requests Preprocess", "Success",
                   f"DM plan requests preprocessing completed successfully. Processed {processed_count} records.")
        return f"SUCCESS: Preprocessed {processed_count} dm_plan_requests records"

    except Exception as e:
        logger.error("DM Plan Requests Preprocess", "Error", f"DM plan requests preprocessing failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed