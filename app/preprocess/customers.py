#!/usr/bin/env python3
"""
ETL Script to extract customer data from CSV and load to Parquet format.
Supports chunked processing for large files.
"""

import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import pandas as pd
    import pyarrow.parquet as pq
    import pyarrow as pa
    from dotenv import load_dotenv
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Please install dependencies using: pip install -r requirements.txt")
    sys.exit(1)

# Import local modules
sys.path.append(str(Path(__file__).parent.parent))
from pull.customers_extractor import extract_customers_from_raw


def transform_customers(df: pd.DataFrame, error_dir: Path) -> pd.DataFrame:
    """
    Transform customer data: clean and validate.
    Invalid records are saved to CSV for debugging.

    Args:
        df: Raw customer DataFrame
        error_dir: Directory to save error records

    Returns:
        Transformed customer DataFrame (valid records only)
    """
    # Keep track of original rows for error detection
    original_df = df.copy()

    # Strip whitespace from string columns
    string_columns = df.select_dtypes(include=['object']).columns
    df[string_columns] = df[string_columns].apply(
        lambda x: x.str.strip() if x.dtype == "object" else x
    )

    # Handle missing values - report but don't modify by default
    missing_counts = df.isnull().sum()
    if missing_counts.any():
        print(f"Warning: Found missing values:\n{missing_counts}")

    # Convert customer_id to integer if it exists and contains numeric data
    if 'customer_id' in df.columns:
        # Keep track of original values for error detection
        original_customer_id = df['customer_id'].copy()
        # Convert to numeric, coercing errors to NaN, then convert to nullable integer
        df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce').astype('Int64')

        # Identify rows where customer_id conversion failed (NaN values)
        invalid_customer_mask = df['customer_id'].isna() & original_customer_id.notna()
        if invalid_customer_mask.any():
            invalid_customers = original_df[invalid_customer_mask].copy()
            invalid_customers['error_reason'] = 'Invalid customer_id (non-numeric or null after cleaning)'
            # Save error records
            error_file = error_dir / f"customers_customer_id_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            invalid_customers.to_csv(error_file, index=False)
            print(f"Warning: Saved {len(invalid_customers)} invalid customer records to {error_file}")

    # Return only valid rows (where customer_id is not null)
    valid_df = df.dropna(subset=['customer_id']) if 'customer_id' in df.columns else df
    return valid_df


def write_to_parquet(df: pd.DataFrame, output_path: Path, write_header: bool = False) -> None:
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


def main():
    """Main ETL function with chunked processing support."""
    # Load environment variables
    load_dotenv()

    # Get configuration from environment variables with defaults
    raw_dir = os.getenv('RAW_DIR', './data/raw')
    staging_dir = os.getenv('STAGING_DIR', './data/staging')
    error_dir = os.getenv('ERROR_DIR', './data/error')  # Directory for error records
    # Chunk size for processing (None means load all at once)
    chunksize_str = os.getenv('CHUNK_SIZE')
    chunksize = int(chunksize_str) if chunksize_str and chunksize_str.isdigit() else None

    # Generate output filename with date and batching info
    today_str = datetime.now().strftime('%Y%m%d')
    entity = 'customers'
    if chunksize is not None:
        output_file = Path(staging_dir) / f'{entity}_{today_str}_batchingper{chunksize}.parquet'
    else:
        output_file = Path(staging_dir) / f'{entity}_{today_str}.parquet'

    # Ensure error directory exists
    error_path = Path(error_dir)
    error_path.mkdir(parents=True, exist_ok=True)

    try:
        # Extract: Pull customer data using the pull module
        print(f"Extracting customer data{' in chunks of ' + str(chunksize) if chunksize else ''}...")
        extracted_data = extract_customers_from_raw(raw_dir, chunksize=chunksize)

        # Initialize counters
        total_rows = 0
        valid_rows = 0
        chunk_count = 0

        # Process data based on whether we got a DataFrame (single chunk) or iterator (multiple chunks)
        if isinstance(extracted_data, pd.DataFrame):
            # Single chunk (backward compatibility)
            print("Processing single chunk...")
            df = transform_customers(extracted_data, error_path)
            row_count = len(df)
            total_rows += len(extracted_data)  # Count original rows before filtering
            valid_rows += row_count
            chunk_count += 1
            print(f"Chunk {chunk_count}: {row_count} valid rows (from {len(extracted_data)} total)")

            # Write to Parquet (new file)
            write_to_parquet(df, output_file, write_header=True)
            print(f"Written chunk {chunk_count} to {output_file}")
        else:
            # Iterate over chunks
            print("Processing chunks...")
            for chunk_df in extracted_data:
                chunk_count += 1
                print(f"Processing chunk {chunk_count}...")
                df = transform_customers(chunk_df, error_path)
                row_count = len(df)
                total_rows += len(chunk_df)  # Count original rows before filtering
                valid_rows += row_count
                print(f"Chunk {chunk_count}: {row_count} valid rows (from {len(chunk_df)} total)")

                # Write chunk to Parquet (first chunk creates file, subsequent chunks append)
                write_to_parquet(df, output_file, write_header=(chunk_count == 1))
                print(f"Written chunk {chunk_count} to {output_file}")

        # Print summary information
        print("\n" + "="*60)
        print("ETL SUMMARY")
        print("="*60)
        print(f"Source: {Path(raw_dir) / 'customers.csv'}")
        print(f"Destination: {output_file}")
        print(f"Error records: {error_path}")
        print(f"Chunk size: {chunksize if chunksize else 'All at once'}")
        print(f"Chunks processed: {chunk_count}")
        print(f"Total rows processed: {total_rows}")
        print(f"Valid rows processed: {valid_rows}")
        print(f"Invalid rows filtered: {total_rows - valid_rows}")
        if chunk_count > 0 and 'df' in locals():
            print(f"Columns: {list(df.columns)}")
            print("\nData types (from last processed chunk):")
            for col, dtype in df.dtypes.items():
                print(f"  {col}: {dtype}")
        else:
            print("Columns: N/A")
            print("\nData types: N/A")

        return True

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the customers.csv file exists in the data/raw directory.")
        return False
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)