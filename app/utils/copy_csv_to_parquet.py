"""
Standalone function to copy CSV file to Parquet format.
No classes, no methods, no project dependencies - pure file conversion.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

def copy_csv_to_parquet(input_csv_path: str, output_parquet_path: str) -> None:
    """
    Copy a CSV file to Parquet format.

    Args:
        input_csv_path: Path to source CSV file
        output_parquet_path: Path for output Parquet file

    Raises:
        FileNotFoundError: If input CSV doesn't exist
        Exception: For pandas/pyarrow errors during conversion
    """
    # Validate input file exists
    input_path = Path(input_csv_path)
    if not input_path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    # Read CSV and convert to Parquet
    df = pd.read_csv(input_path)
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_parquet_path, compression='snappy')