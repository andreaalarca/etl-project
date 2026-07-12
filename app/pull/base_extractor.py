"""
Base extractor for CSV files.
Provides common functionality for extracting data from CSV files.
"""

import os
from pathlib import Path
from typing import Any, Optional, Iterable, Union
import shutil
try:
    import pandas as pd
    from dotenv import load_dotenv
except ImportError:
    # Dependencies will be checked at runtime when used
    pd = None
    load_dotenv = None


class BaseExtractor:
    """
    Base class for extracting data from CSV files.
    """

    def __init__(self, raw_dir: str = None,source_dir: str = None):
        """
        Initialize the extractor.

        Args:
            raw_dir: Path to raw data directory. If None, uses RAW_DIR from environment.
        """
        if load_dotenv is not None:
            load_dotenv()  # Load environment variables from .env file
        self.raw_dir = Path(raw_dir or os.getenv('RAW_DIR', './data/raw'))
        self.source_dir = Path(source_dir or os.getenv('SOURCE_DIR', './data/source'))
        

    def extract(
        self,
        chunksize: int | None = None,
    ) -> Union[pd.DataFrame, Iterable[pd.DataFrame]]:

        if not self.file_name:
            raise ValueError(
                f"{self.__class__.__name__} must define 'file_name'."
            )

        file_path = self.raw_dir / self.file_name

        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        # Determine file type based on extension and use appropriate pandas reader
        file_extension = file_path.suffix.lower()

        def _read_csv_chunks():
            return pd.read_csv(file_path, chunksize=chunksize)

        def _read_csv_full():
            return pd.read_csv(file_path)

        def _read_excel():
            # Excel files don't support chunksize natively, read full file
            if chunksize is not None:
                print(f"Warning: Chunked reading not supported for Excel files. Reading entire file {file_path}")
            return pd.read_excel(file_path)

        def _read_txt_chunks():
            # Assume tab-delimited for .txt files
            return pd.read_table(file_path, sep='\t', chunksize=chunksize)

        def _read_txt_full():
            # Assume tab-delimited for .txt files
            return pd.read_table(file_path, sep='\t')

        def _read_tsv_chunks():
            return pd.read_table(file_path, sep='\t', chunksize=chunksize)

        def _read_tsv_full():
            return pd.read_table(file_path, sep='\t')

        # Map file extensions to reader functions
        if file_extension == '.csv':
            reader = _read_csv_chunks if chunksize is not None else _read_csv_full
        elif file_extension in ['.xlsx', '.xls']:
            reader = _read_excel  # Excel doesn't support chunksize
        elif file_extension == '.tsv':
            reader = _read_tsv_chunks if chunksize is not None else _read_tsv_full
        elif file_extension == '.txt':
            reader = _read_txt_chunks if chunksize is not None else _read_txt_full
        else:
            raise ValueError(f"Unsupported file format: {file_extension}. Supported formats: .csv, .xlsx, .xls, .txt, .tsv")

        return reader()
    
    def copy_to_raw(self,source_path: str, raw_path: str) -> Path:
        """
        Copy a source file to the raw directory.

        Args:
            source_path (str): Path to the source file.
            raw_path (str): Path to the raw directory.

        Returns:
            Path: The copied file path.
        """
        source = Path(source_path)
        raw = Path(raw_path)

        raw.mkdir(parents=True, exist_ok=True)

        destination = raw / source.name

        shutil.copy2(source, destination)

        return destination