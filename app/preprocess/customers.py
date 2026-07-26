import os
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from app.utils.logger import logger


class CustomersPreprocessor:

    # Configuration for missing value handling
    MISSING_VALUE_STRATEGY = {
        'customer_id': 'drop',        # Drop rows with missing IDs
        'customer_name': '',          # Fill with empty string
        'contact_person': '',         # Fill with empty string
        'phone_number': '',           # Fill with empty string
        'email': '',                  # Fill with empty string
        'city': '',                   # Fill with empty string
        'subcontractor_category': '', # Fill with empty string
    }

    def __init__(self):
        self.error_dir = Path(os.getenv('ERROR_DIR'))
        self.error_dir.mkdir(parents=True, exist_ok=True)

        self.staging_dir = Path(os.getenv('STAGING_DIR', './data/staging'))
        # Define preprocessing configuration (matches source columns)
        self.REQUIRED_SOURCE_COLUMNS = [
            'customer_id',
            'customer_name',
            'contact_person',
            'phone_number',
            'email',
            'city',
            'subcontractor_category'
        ]

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        # ===== STEP 1: VALIDATE FILE STRUCTURE =====

        if df.empty:
            raise ValueError("Input file is empty.")

        if len(df.columns) == 0:
            raise ValueError("Input file contains no columns.")

        # ===== STEP 2: NORMALIZE HEADER NAMES =====

        df.columns = (
            df.columns
                .str.strip()
                .str.lower()
                .str.replace(r"[ \-]+", "_", regex=True)
        )

        # ===== STEP 3: VALIDATE SOURCE HEADERS =====

        missing_columns = [
            col
            for col in self.REQUIRED_SOURCE_COLUMNS
            if col not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required source columns: {missing_columns}"
            )

        # ===== STEP 4: CLEAN STRING VALUES =====

        string_columns = df.select_dtypes(include="object").columns

        for column in string_columns:

            df[column] = (
                df[column]
                .astype(str)
                .str.strip()
            )

            df[column] = df[column].replace("nan", pd.NA)
        error_records = []
        # ===== STEP 5: HANDLE MISSING VALUES =====
        for column, strategy in self.MISSING_VALUE_STRATEGY.items():
            if column in df.columns:
                missing_count = df[column].isna().sum()
                if missing_count > 0:
                    logger.warning("Customers Job", "Preprocess", f"Column '{column}' has {missing_count} missing values - applying strategy: '{strategy}'")

                    if strategy == 'drop':
                        # Drop rows where this column is missing
                        df = df.dropna(subset=[column])
                    elif isinstance(strategy, str):
                        # Fill with string value
                        df[column] = df[column].fillna(strategy)
                    elif isinstance(strategy, (int, float)):
                        # Fill with numeric value
                        df[column] = df[column].fillna(strategy)
                    elif isinstance(strategy, bool):
                        # Fill with boolean value
                        df[column] = df[column].fillna(strategy)

                    # Record missing value handling
                    error_records.append({
                        'error_type': 'MISSING_VALUE_HANDLED',
                        'column': column,
                        'strategy': strategy,
                        'count_handled': int(missing_count),
                        'timestamp': datetime.now().isoformat()
                    })

        # Save any error records from this chunk
        if error_records:
            error_df = pd.DataFrame(error_records)
            error_file = self.error_dir / f"customers_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            # Append to existing error file or create new
            if error_file.exists():
                error_df.to_csv(error_file, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file, index=False)
            logger.info("Customers Job", "Preprocess", f"Saved {len(error_records)} error records to {error_file}")

        return df

    def _write_to_parquet(
        self,
        cleaned_df: pd.DataFrame,
        output_file: Path,
        writer: pq.ParquetWriter | None = None,
    ) -> pq.ParquetWriter:
        """
        Write a DataFrame to a parquet file.

        If writer is None:
            - create a new ParquetWriter
        Else:
            - append to the existing writer

        Returns:
            ParquetWriter
        """

        output_file.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pandas(cleaned_df)

        if writer is None:

            writer = pq.ParquetWriter(
                output_file,
                table.schema,
                compression="snappy",
            )

        writer.write_table(table)

        return writer

    def process(
        self,
        df: pd.DataFrame,
        writer: pq.ParquetWriter | None = None,
    ) -> tuple[pd.DataFrame, pq.ParquetWriter]:

        cleaned_df = self._clean_dataframe(df)

        # Create output filename
        today = datetime.now().strftime("%Y%m%d")

        output_file = ( self.staging_dir /
            f"customers_{today}.parquet"
        )

        writer = self._write_to_parquet(
            cleaned_df,
            output_file,
            writer,
        )

        return cleaned_df, writer


def execute() -> str:
    """
    Execute the customer data preprocessing process.
    This method orchestrates the preprocess step:
    1. Read previously extracted data from staging area
    2. Validate and clean the data
    3. Handle missing values according to strategy
    4. Write processed data back to staging area (overwriting input)

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("Customers Preprocess", "Start", "Beginning customer data preprocessing")

        # Step 1: Locate input file from extraction phase
        today = datetime.now().strftime("%Y%m%d")
        input_file = Path(os.getenv('STAGING_DIR', './data/staging')) / f"customers_{today}.parquet"

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}. Ensure extraction step has completed.")

        # Step 2: Read data from staging area
        df = pd.read_parquet(input_file)
        initial_count = len(df)
        logger.info("Customers Preprocess", "Read Complete", f"Read {initial_count} records from {input_file}")

        if df.empty:
            logger.warning("Customers Preprocess", "Empty Input", "Received empty DataFrame from extraction")
            # Still write empty DataFrame to maintain pipeline
            df.to_parquet(input_file, index=False)
            return f"SUCCESS: Preprocessed 0 customer records (empty input)"

        # Step 3: Process data (clean and handle missing values)
        preprocessor = CustomersPreprocessor()
        processed_df, _ = preprocessor.process(df, None)  # We don't need the writer for single write
        processed_count = len(processed_df)

        # Step 4: Write processed data back to staging area (overwrite input)
        processed_df.to_parquet(input_file, index=False)
        logger.info("Customers Preprocess", "Write Complete",
                   f"Wrote {processed_count} processed records to {input_file}")

        # Log filtering info if any rows were removed
        filtered_count = initial_count - processed_count
        if filtered_count > 0:
            logger.info("Customers Preprocess", "Filtering Info",
                       f"Filtered out {filtered_count} records during preprocessing")

        logger.info("Customers Preprocess", "Success",
                   f"Customer preprocessing completed successfully. Processed {processed_count} records.")
        return f"SUCCESS: Preprocessed {processed_count} customer records"

    except Exception as e:
        logger.error("Customers Preprocess", "Error", f"Customer preprocessing failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed