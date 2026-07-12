"""
Customer data extraction module.
Handles pulling/extracting customer data from CSV files.
"""

import pandas as pd
from pathlib import Path
from typing import Optional, Union, Iterable

try:
    # Ensure pandas is available; if not, let import error propagate
    pass
except Exception:
    pass


def _read_customers_csv(file_path: Path, chunksize: int = None) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
    """
    Internal helper to read customer CSV with optional chunking.

    Args:
        file_path: Path to the CSV file
        chunksize: If specified, return iterator of DataFrames; else return full DataFrame

    Returns:
        DataFrame or iterator of DataFrames
    """
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found at {file_path}")

    try:
        if chunksize is None:
            df = pd.read_csv(file_path)
            return df
        else:
            return pd.read_csv(file_path, chunksize=chunksize)
    except Exception as e:
        raise Exception(f"Error reading CSV file {file_path}: {str(e)}")


def extract_customers_csv(file_path: Path, chunksize: int = None) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
    """
    Extract customer data from CSV file.

    Args:
        file_path: Path to the customers.csv file
        chunksize: If specified, return iterator of DataFrames of given size; else return full DataFrame

    Returns:
        DataFrame or iterator of DataFrames

    Raises:
        FileNotFoundError: If the input file doesn't exist
        Exception: For other errors during extraction
    """
    return _read_customers_csv(file_path, chunksize)


def extract_customers_from_raw(raw_dir: str = './data/raw', chunksize: int = None) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
    """
    Extract customer data from the raw data directory.

    Args:
        raw_dir: Path to raw data directory (default: './data/raw')
        chunksize: If specified, return iterator of DataFrames; else return full DataFrame

    Returns:
        DataFrame or iterator of DataFrames
    """
    input_file = Path(raw_dir) / 'customers.csv'
    return _read_customers_csv(input_file, chunksize)


def extract(chunksize: int = None) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:
    """
    Convenience function that extracts customer data using default raw directory.

    Args:
        chunkize: If specified, return iterator of DataFrames; else return full DataFrame

    Returns:
        DataFrame or iterator of DataFrames
    """
    return extract_customers_from_raw(chunksize=chunksize)