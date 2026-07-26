import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime

from app.utils.logger import logger


class CustomersTransformer:

    def __init__(self):
        # No external dependencies needed for customer transformation
        logger.info("Customers Job", "Transform", "Initialized customer transformer")

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform customers data by applying appropriate data types.
        No complex transformations needed for customer lookup table.

        Expected input columns (after preprocessing):
        customer_id, customer_name, contact_person, phone_number, email,
        city, subcontractor_category

        Output columns:
        customer_id, customer_name, contact_person, phone_number, email,
        city, subcontractor_category
        """
        logger.info("Customers Job", "Transform", "Starting customer data transformation")

        if df.empty:
            logger.warning("Customers Job", "Transform", "Received empty DataFrame")
            return df

        df = df.copy()

        # Apply final data type conversions
        # ID columns should be integers
        id_columns = ['customer_id']
        for col in id_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # String columns
        string_columns = ['customer_name', 'contact_person', 'phone_number', 'email',
                         'city', 'subcontractor_category']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype('string')

        logger.info("Customers Job", "Transform", f"Transformation complete. Final shape: {df.shape}")
        return df


def execute() -> str:
    """
    Execute the customer data transformation process.
    This method orchestrates the transform step:
    1. Read preprocessed data from staging area
    2. Apply data type transformations
    3. Write transformed data back to staging area (overwriting input)

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("Customers Transform", "Start", "Beginning customer data transformation")

        # Step 1: Locate input file from preprocessing phase
        today = datetime.now().strftime("%Y%m%d")
        input_file = Path(os.getenv('STAGING_DIR', './data/staging')) / f"customers_{today}.parquet"

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}. Ensure preprocessing step has completed.")

        # Step 2: Read data from staging area
        df = pd.read_parquet(input_file)
        record_count = len(df)
        logger.info("Customers Transform", "Read Complete", f"Read {record_count} records from {input_file}")

        if df.empty:
            logger.warning("Customers Transform", "Empty Input", "Received empty DataFrame from preprocessing")
            # Still write empty DataFrame to maintain pipeline
            df.to_parquet(input_file, index=False)
            return f"SUCCESS: Transformed 0 customer records (empty input)"

        # Step 3: Transform data
        transformer = CustomersTransformer()
        transformed_df = transformer.transform(df)

        # Verify we still have the same number of rows (transformation shouldn't filter rows)
        if len(transformed_df) != record_count:
            logger.warning("Customers Transform", "Row Count Change",
                          f"Row count changed from {record_count} to {len(transformed_df)} during transformation")

        # Step 4: Write transformed data back to staging area (overwrite input)
        transformed_df.to_parquet(input_file, index=False)
        logger.info("Customers Transform", "Write Complete",
                   f"Wrote {len(transformed_df)} transformed records to {input_file}")

        logger.info("Customers Transform", "Success",
                   f"Customer transformation completed successfully. Transformed {len(transformed_df)} records.")
        return f"SUCCESS: Transformed {len(transformed_df)} customer records"

    except Exception as e:
        logger.error("Customers Transform", "Error", f"Customer transformation failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed