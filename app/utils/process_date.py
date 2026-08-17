"""
Process date utilities for ETL pipeline.
Provides function to determine the process date for ETL runs.
"""
from datetime import datetime, timedelta


def get_process_date(execution_date: str = None) -> str:
    """
    Return the process date in YYYY-MM-DD format.
    If execution_date is provided, use it (expected to be YYYY-MM-DD).
    Otherwise, return yesterday's date in YYYY-MM-DD format.

    Args:
        execution_date: Optional process date in YYYY-MM-DD format (e.g., Airflow ds)

    Returns:
        Process date string in YYYY-MM-DD format
    """
    if execution_date is not None:
        return execution_date
    # Default to yesterday
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime("%Y-%m-%d")