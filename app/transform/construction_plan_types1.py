#!/usr/bin/env python3
"""
ETL Script to extract construction plan types data from CSV and load to Parquet format.
Handles validation, cleaning, mapping, and type conversion using a class-based structure.
"""

import os
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from dotenv import load_dotenv

from pull.construction_plan_types_extractor import ConstructionPlanTypesExtractor


class ConstructionPlanTypesPreprocessor:
    """
    Preprocessor for construction plan types data.
    Handles extraction, transformation, and loading of construction plan types data.
    """

    # Configuration for missing value handling
    MISSING_VALUE_STRATEGY = {
        'plan_type_id': 'drop',        # Drop rows with missing IDs
        'plan_type_name': 'unknown',   # Fill with 'unknown'
        'description': '',             # Fill with empty string
        'base_price': 0.0,             # Fill with 0.0
        'is_active': False             # Fill with False
    }

    def __init__(self, raw_dir: str = None, staging_dir: str = None,
                 error_dir: str = None, chunksize: int = None):
        """
        Initialize the preprocessor with directory paths and processing options.

        Args:
            raw_dir: Directory containing source CSV files (defaults to env var or './data/raw')
            staging_dir: Directory for output Parquet files (defaults to env var or './data/staging')
            error_dir: Directory for error records (defaults to env var or './data/error')
            chunksize: Number of rows per chunk for processing (None means load all at once)
        """
        # Load environment variables
        load_dotenv()

        # Set directories from parameters or environment variables with defaults
        self.raw_dir = Path(raw_dir) if raw_dir else Path(os.getenv('RAW_DIR', './data/raw'))
        self.staging_dir = Path(staging_dir) if staging_dir else Path(os.getenv('STAGING_DIR', './data/staging'))
        self.error_dir = Path(error_dir) if error_dir else Path(os.getenv('ERROR_DIR', './data/error'))

        # Create directories if they don't exist
        self.staging_dir.mkdir(parents=True, exist_ok=True)
        self.error_dir.mkdir(parents=True, exist_ok=True)

        # Handle chunksize parameter
        if chunksize is not None:
            self.chunksize = chunksize
        else:
            chunksize_str = os.getenv('CHUNK_SIZE')
            self.chunksize = int(chunksize_str) if chunksize_str and chunksize_str.isdigit() else None

        # Define preprocessing configuration (matches original script)
        self.REQUIRED_SOURCE_COLUMNS = [
            'plan_type_id',
            'plan_type_name',
            'description',
            'base_price',
            'is_active'
        ]

        self.COLUMN_MAPPING = {
            'plan_type_id': 'plan_type_id',
            'plan_type_name': 'plan_type_name',
            'description': 'description',
            'base_price': 'base_price',
            'is_active': 'is_active'
        }

        self.DTYPE_CONVERSIONS = {
            'plan_type_id': 'Int64',
            'base_price': 'float64',
            'is_active': 'boolean'
        }

    def _transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform a single DataFrame chunk: validate, clean, map, convert types, and handle missing values.

        Args:
            df: Raw DataFrame chunk

        Returns:
            Transformed DataFrame (cleaned and ready for loading)
        """
        # Make a copy to avoid modifying original
        df = df.copy()
        error_records = []  # Track errors for this chunk

        # ===== STEP 1: VALIDATE REQUIRED COLUMNS =====
        missing_columns = [col for col in self.REQUIRED_SOURCE_COLUMNS if col not in df.columns]
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            print(f"ERROR: {error_msg}")
            # Save error record for missing columns
            error_df = pd.DataFrame([{
                'error_type': 'MISSING_COLUMNS',
                'error_message': error_msg,
                'timestamp': datetime.now().isoformat(),
                'chunk_rows': len(df)
            }])
            error_file = self.error_dir / f"construction_plan_types_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            if error_file.exists():
                error_df.to_csv(error_file, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file, index=False)
            raise ValueError(error_msg)

        # ===== STEP 2: CLEAN HEADER NAMES =====
        # Remove leading/trailing whitespace, convert to lowercase, replace spaces/hyphens with underscores
        df.columns = df.columns.str.strip().str.lower().str.replace(r'[ \-]+', '_', regex=True)

        # ===== STEP 3: MAP SOURCE HEADERS TO SQL COLUMN NAMES =====
        # Rename columns according to mapping (only keeps mapped columns)
        df = df.rename(columns=self.COLUMN_MAPPING)

        # ===== STEP 4: CONVERT DATA TYPES =====
        for column, target_dtype in self.DTYPE_CONVERSIONS.items():
            if column in df.columns:
                try:
                    if target_dtype == 'Int64':
                        # Handle nullable integer conversion
                        df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                    elif target_dtype == 'boolean':
                        # Handle boolean conversion (supports True/False, 1/0, 'true'/'false', etc.)
                        df[column] = df[column].astype(str).str.lower().map({
                            'true': True, 'false': False, '1': True, '0': False
                        }).fillna(False)  # Default False for unrecognized values
                    else:
                        df[column] = df[column].astype(target_dtype)
                except Exception as e:
                    print(f"Warning: Failed to convert column '{column}' to {target_dtype}: {str(e)}")
                    # Record conversion errors
                    error_records.append({
                        'error_type': 'DTYPE_CONVERSION_FAILED',
                        'column': column,
                        'target_dtype': target_dtype,
                        'error_message': str(e),
                        'timestamp': datetime.now().isoformat()
                    })

        # ===== STEP 5: HANDLE MISSING VALUES =====
        for column, strategy in self.MISSING_VALUE_STRATEGY.items():
            if column in df.columns:
                missing_count = df[column].isna().sum()
                if missing_count > 0:
                    print(f"Warning: Column '{column}' has {missing_count} missing values - applying strategy: '{strategy}'")

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
            error_file = self.error_dir / f"construction_plan_types_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            # Append to existing error file or create new
            if error_file.exists():
                error_df.to_csv(error_file, mode='a', header=False, index=False)
            else:
                error_df.to_csv(error_file, index=False)

        return df

    def _write_to_parquet(self, df: pd.DataFrame, output_path: Path, write_header: bool = False) -> None:
        """
        Write DataFrame to Parquet file, creating or appending as needed.

        Args:
            df: DataFrame to write
            output_path: Path to the Parquet file
            write_header: If True, write as new file (including metadata); if False, append to existing file
        """
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        table = pa.Table.from_pandas(df)

        if write_header or not output_path.exists():
            # Write new file
            pq.write_table(table, output_path, compression='snappy')
        else:
            # Append to existing file
            with pq.ParquetWriter(output_path, table.schema, compression='snappy') as writer:
                writer.write_table(table)

    def process(self) -> bool:
        """
        Execute the full preprocessing pipeline: extract, transform, and load to Parquet.

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Generate output filename with date and batching info
            today_str = datetime.now().strftime('%Y%m%d')
            entity = 'construction_plan_types'
            if self.chunksize is not None:
                output_file = self.staging_dir / f'{entity}_{today_str}_batchingper{self.chunksize}.parquet'
            else:
                output_file = self.staging_dir / f'{entity}_{today_str}.parquet'

            print(f"Starting preprocessing for {entity}...")
            print(f"Source directory: {self.raw_dir}")
            print(f"Staging directory: {self.staging_dir}")
            print(f"Error directory: {self.error_dir}")
            print(f"Chunk size: {self.chunksize if self.chunksize else 'All at once'}")

            # Extract: Pull construction plan types data
            print(f"Extracting construction plan types data{' in chunks of ' + str(self.chunksize) if self.chunksize else ''}...")
            extractor = ConstructionPlanTypesExtractor(raw_dir=str(self.raw_dir), staging_dir=str(self.staging_dir))
            extracted_data = extractor.extract(chunksize=self.chunksize)

            # Initialize counters
            total_rows = 0
            chunk_count = 0

            # Process data based on whether we got a DataFrame (single chunk) or iterator (multiple chunks)
            if isinstance(extracted_data, pd.DataFrame):
                # Single chunk (backward compatibility)
                print("Processing single chunk...")
                df = self._transform_dataframe(extracted_data)
                row_count = len(df)
                total_rows += row_count
                chunk_count += 1
                print(f"Chunk {chunk_count}: {row_count} rows")

                # Write to Parquet (new file)
                self._write_to_parquet(df, output_file, write_header=True)
                print(f"Written chunk {chunk_count} to {output_file}")
            else:
                # Iterate over chunks
                print("Processing chunks...")
                for chunk_df in extracted_data:
                    chunk_count += 1
                    print(f"Processing chunk {chunk_count}...")
                    df = self._transform_dataframe(chunk_df)
                    row_count = len(df)
                    total_rows += row_count
                    print(f"Chunk {chunk_count}: {row_count} rows")

                    # Write chunk to Parquet (first chunk creates file, subsequent chunks append)
                    self._write_to_parquet(df, output_file, write_header=(chunk_count == 1))
                    print(f"Written chunk {chunk_count} to {output_file}")

            # Print summary information
            print("\n" + "="*60)
            print("PREPROCESSING SUMMARY - CONSTRUCTION PLAN TYPES")
            print("="*60)
            print(f"Source: {self.raw_dir / 'construction_plan_types.csv'}")
            print(f"Destination: {output_file}")
            print(f"Error records: {self.error_dir}")
            print(f"Chunk size: {self.chunksize if self.chunksize else 'All at once'}")
            print(f"Chunks processed: {chunk_count}")
            print(f"Total rows processed: {total_rows}")
            if chunk_count > 0 and 'df' in locals():
                print(f"Columns: {list(df.columns)}")
                print("\nData types (from last processed chunk):")
                for col, dtype in df.dtypes.items():
                    print(f"  {col}: {dtype}")
            print("="*60)

            return True

        except FileNotFoundError as e:
            print(f"Error: {e}")
            print("Please ensure the construction_plan_types.csv file exists in the data/raw directory.")
            return False
        except Exception as e:
            print(f"Error during preprocessing: {str(e)}")
            import traceback
            traceback.print_exc()
            return False


def main() -> bool:
    """
    Main function for backward compatibility with existing workflows.
    Creates a preprocessor instance and runs the processing pipeline.

    Returns:
        bool: True if successful, False otherwise
    """
    preprocessor = ConstructionPlanTypesPreprocessor()
    return preprocessor.process()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)