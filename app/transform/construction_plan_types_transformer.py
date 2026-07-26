import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

from app.utils.logger import logger


class ConstructionPlanTypesTransformer:

    def __init__(self):

        # self.staging_dir = Path(
        #     os.getenv("STAGING_DIR", "./data/staging")
        # )

        # Source -> SQL mapping
        self.COLUMN_MAPPING = {
            "plan_type_name": "plan_type",
            "plan_type_id": "plan_type_id",
            "plan_category": "plan_category",
            "required_input": "required_input",
            "output_format": "output_format",
            "complexity_level": "complexity_level",
            "base_price": "base_price",
        }

        # SQL schema
        self.SQL_COLUMNS = [
            "plan_type_id",
            "plan_type",
            "plan_category",
            "required_input",
            "output_format",
            "complexity_level",
            "base_price",
        ]

        self.DEFAULT_VALUES = {
            "plan_category": None,
            "required_input": None,
            "output_format": None,
            "complexity_level": None,
            "base_price": 0.0,
        }

        self.SQL_DTYPES = {
            "plan_type_id": "Int64",
            "plan_type": "string",
            "plan_category": "string",
            "required_input": "string",
            "output_format": "string",
            "complexity_level": "string",
            "base_price": "float64",
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Construction Plan Types Job", "Transform", "Starting data transformation")

        df = df.copy()

        # ------------------------------------
        # Map source headers
        # ------------------------------------

        df.rename(
            columns=self.COLUMN_MAPPING,
            inplace=True,
        )

        # ------------------------------------
        # Validate SQL columns
        # ------------------------------------

        required = [
            "plan_type_id",
            "plan_type",
        ]

        missing = [
            c
            for c in required
            if c not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Missing SQL columns: {missing}"
            )

        # ------------------------------------
        # Add missing destination columns
        # ------------------------------------

        for column in self.SQL_COLUMNS:

            if column not in df.columns:

                df[column] = self.DEFAULT_VALUES.get(column)

        # ------------------------------------
        # Remove unnecessary columns
        # ------------------------------------

        df = df[self.SQL_COLUMNS]

        # ------------------------------------
        # Convert SQL datatypes
        # ------------------------------------

        df["plan_type_id"] = (
            pd.to_numeric(
                df["plan_type_id"],
                errors="coerce",
            )
            .astype("Int64")
        )

        df["base_price"] = (
            pd.to_numeric(
                df["base_price"],
                errors="coerce",
            )
            .fillna(0)
            .astype("float64")
        )

        for col in [
            "plan_type",
            "plan_category",
            "required_input",
            "output_format",
            "complexity_level",
        ]:

            df[col] = (
                df[col]
                .fillna("")
                .astype("string")
            )

        # ------------------------------------
        # Business rules
        # ------------------------------------

        df = df.dropna(
            subset=["plan_type_id"]
        )

        df.loc[
            df["base_price"] < 0,
            "base_price",
        ] = 0

        # ------------------------------------
        # Derived columns
        # ------------------------------------

        # Example
        # df["created_at"] = pd.Timestamp.now()

        logger.info("Construction Plan Types Job", "Transform", f"Transformation complete. Shape: {df.shape}")
        return df


def execute() -> str:
    """
    Execute the construction plan types data transformation process.
    This method orchestrates the transform step:
    1. Read preprocessed data from staging area
    2. Apply data type transformations
    3. Write transformed data back to staging area (overwriting input)

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("Construction Plan Types Transform", "Start", "Beginning construction plan types data transformation")

        # Step 1: Locate input file from preprocessing phase
        today = datetime.now().strftime("%Y%m%d")
        input_file = Path(os.getenv('STAGING_DIR', './data/staging')) / f"construction_plan_types_{today}.parquet"

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}. Ensure preprocessing step has completed.")

        # Step 2: Read data from staging area
        df = pd.read_parquet(input_file)
        record_count = len(df)
        logger.info("Construction Plan Types Transform", "Read Complete", f"Read {record_count} records from {input_file}")

        if df.empty:
            logger.warning("Construction Plan Types Transform", "Empty Input", "Received empty DataFrame from preprocessing")
            # Still write empty DataFrame to maintain pipeline
            df.to_parquet(input_file, index=False)
            return f"SUCCESS: Transformed 0 construction plan types records (empty input)"

        # Step 3: Transform data
        transformer = ConstructionPlanTypesTransformer()
        transformed_df = transformer.transform(df)

        # Verify we still have the same number of rows (transformation shouldn't filter rows for this table)
        if len(transformed_df) != record_count:
            logger.warning("Construction Plan Types Transform", "Row Count Change",
                          f"Row count changed from {record_count} to {len(transformed_df)} during transformation")

        # Step 4: Write transformed data back to staging area (overwrite input)
        transformed_df.to_parquet(input_file, index=False)
        logger.info("Construction Plan Types Transform", "Write Complete",
                   f"Wrote {len(transformed_df)} transformed records to {input_file}")

        logger.info("Construction Plan Types Transform", "Success",
                   f"Construction plan types transformation completed successfully. Transformed {len(transformed_df)} records.")
        return f"SUCCESS: Transformed {len(transformed_df)} construction plan types records"

    except Exception as e:
        logger.error("Construction Plan Types Transform", "Error", f"Construction plan types transformation failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed