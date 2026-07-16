

import os
import sys
from datetime import datetime
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from app.pull.construction_plan_types_extractor import (
    ConstructionPlanTypesExtractor,
)
from app.utils.logger import logger
class ConstructionPlanTypesPreprocessor:

    # Configuration for missing value handling
    MISSING_VALUE_STRATEGY = {
        'plan_type_id': 'drop',        # Drop rows with missing IDs
        'plan_type': '',   # Fill with ''
        'plan_category': '',   # Fill with ''
        'required_input': '',   # Fill with ''
        'output_format': '',   # Fill with ''
        'complexity_level': '',   # Fill with ''
        'base_price': 0.0,             # Fill with 0.0
    }
    
    def __init__(self):
        self.extractor = ConstructionPlanTypesExtractor()
        
        self.error_dir = Path(os.getenv('ERROR_DIR'))
        self.error_dir.mkdir(parents=True, exist_ok=True)

        self.staging_dir = Path(os.getenv('STAGING_DIR', './data/staging'))
        # Define preprocessing configuration (matches original script)
        self.REQUIRED_SOURCE_COLUMNS = [
            'plan_type_id',        # Drop rows with missing IDs
            'plan_type',
            'plan_category',
            'required_input',
            'output_format',
            'complexity_level',
            'base_price'
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
                    logger.warning("Construction Plan Types Job", "Preprocess", f"Column '{column}' has {missing_count} missing values - applying strategy: '{strategy}'")

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
            logger.info("Construction Plan Types Job", "Preprocess", f"Saved {len(error_records)} error records to {error_file}")

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
    
    
    # def _write_to_parquet(
    #     self,
    #     df: pd.DataFrame,
    #     output_path: Path,
    # ) -> None:
    #     """
    #     Write a DataFrame to a Parquet file.

    #     Args:
    #         df: Cleaned DataFrame.
    #         output_path: Full output parquet path.
    #     """

    #     output_path.parent.mkdir(parents=True, exist_ok=True)

    #     table = pa.Table.from_pandas(df)

    #     pq.write_table(
    #         table,
    #         output_path,
    #         compression="snappy",
    #     )

    #     print(f"Parquet file written to: {output_path}")
    
    def process(
        self,
        df: pd.DataFrame,
        writer: pq.ParquetWriter | None = None,
    ) -> tuple[pd.DataFrame, pq.ParquetWriter]:

        cleaned_df = self._clean_dataframe(df)

        
    # Create output filename
        # today = datetime.now().strftime("%Y%m%d")

        today = datetime.now().strftime("%Y%m%d")

        output_file = ( self.staging_dir /
            f"construction_plan_types_{today}.parquet"
        )

        writer = self._write_to_parquet(
            cleaned_df,
            output_file,
            writer,
        )

        return cleaned_df, writer


# if __name__ == "__main__":

#     preprocessor = ConstructionPlanTypesPreprocessor()

#     # Extract raw data
#     df = preprocessor.extractor.extract()

#     # Create output filename
#     today = datetime.now().strftime("%Y%m%d")

#     output_file = (
#         preprocessor.staging_dir
#         / f"construction_plan_types_{today}.parquet"
#     )

#     # Process and write parquet
#     cleaned_df = preprocessor.process(
#         df=df,
#         output_file=output_file,
#     )

#     print("\n=== Preview ===")
#     print(cleaned_df.head())

#     print(f"\nRows processed: {len(cleaned_df)}")