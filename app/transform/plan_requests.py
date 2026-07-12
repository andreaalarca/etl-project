#!/usr/bin/env python3
"""
ETL Script to extract plan requests data from CSV and load to Parquet format.
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
from pull.plan_requests_extractor import PlanRequestsExtractor, extract as extract_plan_requests


def transform_plan_requests(df: pd.DataFrame, error_dir: Path) -> pd.DataFrame:
    """
    Transform plan requests data: clean and validate.
    Invalid records are saved to CSV for debugging.

    Args:
        df: Raw plan requests DataFrame
        error_dir: Directory to save error records

    Returns:
        Transformed plan requests DataFrame (valid records only)
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

    # Track invalid rows for each validation
    invalid_rows = []

    # Define which ID columns are required (cannot be null after conversion)
    required_id_columns = ['request_id', 'customer_id', 'plan_type_id']

    # Convert ID columns to integers if they exist and contain numeric data
    id_columns = ['request_id', 'customer_id', 'plan_type_id']
    for col in id_columns:
        if col in df.columns:
            # Keep track of original values for error detection
            original_col = df[col].copy()
            # Convert to numeric, coercing errors to NaN, then convert to nullable integer
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

            # Identify rows where ID column conversion failed (NaN values)
            invalid_id_mask = df[col].isna() & original_col.notna()
            if invalid_id_mask.any():
                invalid_id = original_df[invalid_id_mask].copy()
                invalid_id['error_reason'] = f'Invalid {col} (non-numeric or null after cleaning)'
                invalid_rows.append(invalid_id)

    # Convert numeric columns
    numeric_columns = ['floor_area_sqm', 'revision_count']
    for col in numeric_columns:
        if col in df.columns:
            # Keep track of original values for error detection
            original_col = df[col].copy()
            # Convert to numeric, coercing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')

            # Identify rows where numeric column conversion failed (NaN values)
            # For non-required fields, we log but don't invalidate the record
            invalid_numeric_mask = df[col].isna() & original_col.notna()
            if invalid_numeric_mask.any():
                invalid_numeric = original_df[invalid_numeric_mask].copy()
                invalid_numeric['error_reason'] = f'Invalid {col} (non-numeric or null after cleaning)'
                # Only add to invalid rows if it's also a required field (it's not, so we don't add to invalid_rows for filtering)
                # But we still want to log it happened
                pass  # We'll handle logging separately if needed

    # Convert date columns to datetime if they exist
    date_columns = ['request_date', 'target_date', 'completed_date']
    for col in date_columns:
        if col in df.columns:
            # Keep track of original values for error detection
            original_col = df[col].copy()
            # Convert to datetime, coercing errors to NaT
            df[col] = pd.to_datetime(df[col], errors='coerce')

            # Identify rows where date column conversion failed (NaT values)
            # For non-required fields, we log but don't invalidate the record
            invalid_date_mask = df[col].isna() & original_col.notna()
            if invalid_date_mask.any():
                invalid_date = original_df[invalid_date_mask].copy()
                invalid_date['error_reason'] = f'Invalid {col} (invalid date format or null after cleaning)'
                # Only add to invalid rows if it's also a required field (it's not, so we don't add to invalid_rows for filtering)
                # But we still want to log it happened
                pass  # We'll handle logging separately if needed

    # Filter out rows where REQUIRED ID columns are null (invalid)
    # Build a mask of rows to keep (those where ALL required ids are NOT null)
    if required_id_columns:
        # Create a mask that is True for rows we want to keep
        valid_mask = pd.Series(True, index=df.index)
        for col in required_id_columns:
            if col in df.columns:
                valid_mask = valid_mask & df[col].notna()

        # Get the invalid rows (those that fail the required check)
        truly_invalid_df = original_df[~valid_mask].copy()
        if len(truly_invalid_df) > 0:
            # Add error reasons to the truly invalid rows
            error_reasons = []
            for idx in truly_invalid_df.index:
                reasons = []
                for col in required_id_columns:
                    if col in df.columns and pd.isna(df.loc[idx, col]):
                        orig_val = original_df.loc[idx, col]
                        if pd.notna(orig_val):
                            reasons.append(f'Invalid {col} (non-numeric or null after cleaning)')
                error_reasons.append("; ".join(reasons) if reasons else "Invalid or missing required ID")

            truly_invalid_df['error_reason'] = error_reasons
            invalid_rows.append(truly_invalid_df)

    # Combine all invalid rows and save them
    if invalid_rows:
        all_invalid = pd.concat(invalid_rows, ignore_index=True)
        # Remove duplicates (a row might be invalid for multiple reasons)
        all_invalid = all_invalid.drop_duplicates()
        error_file = error_dir / f"plan_requests_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        all_invalid.to_csv(error_file, index=False)
        print(f"Warning: Saved {len(all_invalid)} invalid records to {error_file}")

    # Return only the valid rows (where required IDs are not null)
    if required_id_columns:
        # Recreate the valid mask to filter the dataframe
        valid_mask = pd.Series(True, index=df.index)
        for col in required_id_columns:
            if col in df.columns:
                valid_mask = valid_mask & df[col].notna()
        return df[valid_mask].copy()
    else:
        return df


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
    entity = 'plan_requests'
    if chunksize is not None:
        output_file = Path(staging_dir) / f'{entity}_{today_str}_batchingper{chunksize}.parquet'
    else:
        output_file = Path(staging_dir) / f'{entity}_{today_str}.parquet'

    # Ensure error directory exists
    error_path = Path(error_dir)
    error_path.mkdir(parents=True, exist_ok=True)

    try:
        # Extract: Pull plan requests data using the extractor
        print(f"Extracting plan requests data{' in chunks of ' + str(chunksize) if chunksize else ''}...")
        # Use convenience function that respects chunksize
        extracted_data = extract_plan_requests(chunksize=chunksize)

        # Initialize counters
        total_rows = 0
        valid_rows = 0
        chunk_count = 0

        # Process data based on whether we got a DataFrame (single chunk) or iterator (multiple chunks)
        if isinstance(extracted_data, pd.DataFrame):
            # Single chunk (backward compatibility)
            print("Processing single chunk...")
            df = transform_plan_requests(extracted_data, error_path)
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
                df = transform_plan_requests(chunk_df, error_path)
                row_count = len(df)
                total_rows += len(chunk_df)  # Count original rows before filtering
                valid_rows += row_count
                print(f"Chunk {chunk_count}: {row_count} valid rows (from {len(chunk_df)} total)")

                # Write chunk to Parquet (first chunk creates file, subsequent chunks append)
                write_to_parquet(df, output_file, write_header=(chunk_count == 1))
                print(f"Written chunk {chunk_count} to {output_file}")

        # Print summary information
        print("\n" + "="*60)
        print("ETL SUMMARY - PLAN REQUESTS")
        print("="*60)
        print(f"Source: {Path(raw_dir) / 'plan_requests.csv'}")
        print(f"Destination: {output_file}")
        print(f"Error records: {error_path}")
        print(f"Chunk size: {chunksize if chunksize else 'All at once'}")
        print(f"Chunks processed: {chunk_count}")
        print(f"Total rows processed: {total_rows}")
        print(f"Valid rows processed: {valid_rows}")
        print(f"Invalid rows filtered: {total_rows - valid_rows}")
        print(f"Columns: {list(df.columns) if chunk_count > 0 else 'N/A'}")
        print("\nData types (from last processed chunk):")
        for col, dtype in df.dtypes.items():
            print(f"  {col}: {dtype}")

        return True

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the plan_requests.csv file exists in the data/raw directory.")
        return False
    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)