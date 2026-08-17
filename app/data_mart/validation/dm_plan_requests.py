"""
Data Mart validation for dm_plan_requests.
Responsible for validating the staged Parquet file before loading.
"""
import os
from pathlib import Path
import pandas as pd

from app.utils.logger import logger


def execute(staging_file_path: str, process_date: str = None) -> dict:
    """
    Validate the dm_plan_requests staging Parquet file.

    Args:
        staging_file_path: Path to the staging Parquet file
        process_date: Process date in YYYY-MM-DD format. If None, defaults to yesterday.

    Returns:
        dict with keys:
            - staging_file_path: str, same as input
            - process_date: str, the process date used (YYYY-MM-DD)
            - row_count: int, number of rows in the staging file

    Raises:
        ValueError: If validation fails
        FileNotFoundError: If staging file does not exist
    """
    # Determine process date (if not provided, we still need it for validation)
    from app.utils.process_date import get_process_date
    process_date = get_process_date(process_date)
    logger.info("DM Plan Requests Validation", "Start", f"Beginning validation for process date {process_date}")

    # Check if file exists
    staging_path = Path(staging_file_path)
    if not staging_path.is_file():
        raise FileNotFoundError(f"Staging file not found: {staging_file_path}")

    # Read the staging file
    try:
        df = pd.read_parquet(staging_path)
    except Exception as e:
        raise ValueError(f"Failed to read staging Parquet file: {str(e)}")

    row_count = len(df)
    logger.info("DM Plan Requests Validation", "Read File", f"Read {row_count} rows from {staging_file_path}")

    # Check for unexpected empty result
    if row_count == 0:
        raise ValueError("Staging file contains no data (empty DataFrame)")

    # Expected columns (in order as per specification)
    expected_columns = [
        'request_id',
        'customer_id',
        'customer_name',
        'contact_person',
        'phone_number',
        'email',
        'city',
        'subcontractor_category',
        'plan_type_id',
        'plan_type',
        'plan_category',
        'required_input',
        'output_format',
        'complexity_level',
        'base_price',
        'request_date',
        'target_date',
        'completed_date',
        'status',
        'priority',
        'assigned_coordinator',
        'floor_area_sqm',
        'revision_count'
    ]

    # Check for missing columns
    missing_columns = set(expected_columns) - set(df.columns)
    if missing_columns:
        raise ValueError(f"Missing columns in staging data: {missing_columns}")

    # Check for unexpected columns
    unexpected_columns = set(df.columns) - set(expected_columns)
    if unexpected_columns:
        raise ValueError(f"Unexpected columns in staging data: {unexpected_columns}")

    # Check for duplicate columns (Pandas should not have duplicate column names, but just in case)
    if len(df.columns) != len(set(df.columns)):
        dup_cols = [col for col in df.columns if list(df.columns).count(col) > 1]
        raise ValueError(f"Duplicate columns found in staging data: {dup_cols}")

    # Check required/non-null fields
    required_columns = [
        'request_id',
        'customer_id',
        'customer_name',
        'contact_person',
        'phone_number',
        'email',
        'city',
        'subcontractor_category',
        'plan_type_id',
        'plan_type',
        'plan_category',
        'required_input',
        'output_format',
        'complexity_level',
        'base_price',
        'request_date',
        'status',
        'priority',
        'assigned_coordinator',
        'floor_area_sqm',
        'revision_count'
    ]
    null_counts = df[required_columns].isnull().sum()
    columns_with_nulls = null_counts[null_counts > 0]
    if len(columns_with_nulls) > 0:
        raise ValueError(f"Required columns contain null values: {columns_with_nulls.to_dict()}")

    # Check unique request_id
    if df['request_id'].duplicated().any():
        dup_count = df['request_id'].duplicated().sum()
        raise ValueError(f"Found {dup_count} duplicate request_id values")

    # Check data type validity where practical
    # We'll do basic type checks: request_id, customer_id, plan_type_id should be numeric
    # We'll try to convert to numeric and see if any non-numeric values (but note they might be nullable? but we already checked required)
    try:
        pd.to_numeric(df['request_id'], errors='raise')
        pd.to_numeric(df['customer_id'], errors='raise')
        pd.to_numeric(df['plan_type_id'], errors='raise')
    except Exception as e:
        raise ValueError(f"ID columns must be numeric: {str(e)}")

    # Check process_date consistency: request_date column should equal process_date
    # We expect all request_date values to be equal to the process_date
    # Convert request_date to string in YYYY-MM-DD for comparison
    if 'request_date' in df.columns:
        # Ensure it's datetime
        if not pd.api.types.is_datetime64_any_dtype(df['request_date']):
            try:
                df['request_date'] = pd.to_datetime(df['request_date'])
            except Exception as e:
                raise ValueError(f"request_date column must be convertible to datetime: {str(e)}")
        # Get unique dates in the request_date column
        unique_request_dates = df['request_date'].dt.date.unique()
        if len(unique_request_dates) != 1:
            raise ValueError(f"request_date column contains multiple unique dates: {unique_request_dates}")
        # Convert the single unique date to string
        unique_date_str = unique_request_dates[0].strftime('%Y-%m-%d')
        if unique_date_str != process_date:
            raise ValueError(
                f"request_date column values do not match process_date. "
                f"Expected: {process_date}, found in data: {unique_date_str}"
            )

    logger.info("DM Plan Requests Validation", "Success", f"Validation passed for {row_count} rows")
    return {
        "staging_file_path": str(staging_path),
        "process_date": process_date,
        "row_count": row_count
    }