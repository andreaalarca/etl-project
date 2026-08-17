"""
Staging utilities for ETL pipeline.
Provides functions for managing staging file paths and archiving.
"""
import os
import shutil
from datetime import datetime
from pathlib import Path


def get_staging_file_path(table_name: str, process_date: str, staging_dir: str = None) -> Path:
    """
    Returns the full path to the staging Parquet file for the given table and process date.

    Args:
        table_name: Name of the table (e.g., 'dm_plan_requests')
        process_date: Process date in YYYY-MM-DD format
        staging_dir: Base staging directory. If None, uses STAGING_DIR env var or './data/staging'

    Returns:
        Path object pointing to the staging file
    """
    if staging_dir is None:
        staging_dir = os.getenv("STAGING_DIR", "./data/staging")
    # Convert process_date from YYYY-MM-DD to YYYYMMDD for file naming
    date_file_str = process_date.replace("-", "")
    return Path(staging_dir) / f"{table_name}_{date_file_str}.parquet"


def archive_staging_file(source_file: str | Path, table_name: str, archive_base_dir: str = None) -> Path:
    """
    Archive the staging file to a timestamped subdirectory under the archive base directory for the given table.

    Args:
        source_file: Path to the staging file to archive (as string or Path object)
        table_name: Name of the table (e.g., 'dm_plan_requests')
        archive_base_dir: Base archive directory. If None, uses ARCHIVE_DIR env var or './data/archive'

    Returns:
        Path to the archived file
    """
    if archive_base_dir is None:
        archive_base_dir = os.getenv("ARCHIVE_DIR", "./data/archive")
    # Ensure source_file is a Path object
    source_file_path = Path(source_file)
    # Create a timestamped directory for this table run
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    destination_dir = Path(archive_base_dir) / table_name / timestamp
    destination_dir.mkdir(parents=True, exist_ok=True)
    destination = destination_dir / source_file_path.name
    shutil.move(str(source_file_path), str(destination))
    return destination