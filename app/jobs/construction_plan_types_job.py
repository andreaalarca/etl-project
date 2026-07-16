#!/usr/bin/env python3
"""
ETL Job for Construction Plan Types data.
Orchestrates the extraction, transformation, and loading of construction plan types data.
Handles file acquisition, format detection, and archiving of source files.
"""

import argparse
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Add the app directory to the path so we can import from app modules
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd

from app.pull.construction_plan_types_extractor import (
    extract_construction_plan_types,
)
from app.preprocess.construction_plan_types import (
    ConstructionPlanTypesPreprocessor,
)
from app.transform.construction_plan_types_transformer import (
    ConstructionPlanTypesTransformer,
)
from app.load.construction_plan_types_loader import (
    ConstructionPlanTypesLoader,
)
from app.utils.logger import logger


def _move_source_file_to_error(source_file: Path, error_reason: str, error_detail: str = "") -> None:
    """
    Move the source file to the error directory for failed processing.

    Args:
        source_file: Path to the source file that failed processing
        error_reason: Brief reason for the failure (e.g., "DATA_QUALITY", "TRANSIENT_ERROR")
        error_detail: Detailed error message for logging
    """
    try:
        # Get error directory from environment or use default
        error_dir = Path(os.getenv('ERROR_DIR', './data/error'))
        error_dir.mkdir(parents=True, exist_ok=True)

        # Create a unique filename for the error file to avoid overwriting
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        error_filename = f"{source_file.stem}_{timestamp}{source_file.suffix}"
        error_file_path = error_dir / error_filename

        # Move the source file to error directory
        shutil.move(str(source_file), str(error_file_path))

        # Log the failure
        log_message = (
            f"FILE_MOVED_TO_ERROR | "
            f"Source: {source_file} | "
            f"Error Reason: {error_reason} | "
            f"Error Details: {error_detail} | "
            f"Moved To: {error_file_path} | "
            f"Timestamp: {datetime.now().isoformat()}"
        )
        print(log_message)

        # Also write to a log file for persistence
        log_file = error_dir / "etl_error_log.txt"
        with open(log_file, "a") as f:
            f.write(log_message + "\n")

    except Exception as move_error:
        # If we can't move the file, at least log the error
        print(f"FAILED TO MOVE SOURCE FILE TO ERROR DIRECTORY: {move_error}")
        print(f"Original source file: {source_file}")
        print(f"Original error: {error_detail}")


def main(chunksize: int | None = None, dryrun: str | None = None) -> None:
    # Set DRY_RUN environment variable based on command line argument
    if dryrun is not None:
        os.environ['DRY_RUN'] = str(dryrun).lower()
        logger.info("Construction Plan Types Job", "Extract", f"Set DRY_RUN environment variable to: {os.environ['DRY_RUN']}")
    else:
        # Explicitly set to false to override any environment variable
        os.environ['DRY_RUN'] = 'false'
        logger.info("Construction Plan Types Job", "Extract", f"Set DRY_RUN environment variable to: {os.environ['DRY_RUN']}")

    # Determine the source file path for error handling
    source_dir = Path(os.getenv('SOURCE_DIR', './data/source'))
    source_file_name = "construction_plan_types.csv"  # From ConstructionPlanTypesExtractor.file_name
    source_file = source_dir / source_file_name

    logger.info("Construction Plan Types Job", "Extract", f"Starting ETL job for source file: {source_file}")

    preprocess = ConstructionPlanTypesPreprocessor()
    transformer = ConstructionPlanTypesTransformer()
    loader = ConstructionPlanTypesLoader()

    writer = None
    transformed_df = None
    total_inserted = 0

    # Get the data from extractor (this includes copying source to raw)
    logger.info("Construction Plan Types Job", "Extract", "Starting data extraction")
    data = extract_construction_plan_types(chunksize=chunksize)

    try:
        if isinstance(data, pd.DataFrame):
            # Non-chunked: process single DataFrame
            logger.info("Construction Plan Types Job", "Preprocess", "Starting preprocessing of data")
            clean, writer = preprocess.process(
                data,
                writer,
            )

            logger.info("Construction Plan Types Job", "Transform", "Starting transformation of data")
            transformed_df = transformer.transform(clean)

            # Load the transformed data
            logger.info("Construction Plan Types Job", "Load", f"Loading {len(transformed_df)} rows into warehouse")
            total_inserted = loader.load(transformed_df)
            logger.info("Construction Plan Types Job", "Load", f"Successfully inserted {total_inserted} rows")

        else:
            # Chunked: process each chunk
            logger.info("Construction Plan Types Job", "Extract", f"Processing data in chunks of size {chunksize}")
            for i, chunk in enumerate(data, start=1):
                logger.info("Construction Plan Types Job", "Extract", f"Processing chunk {i}")

                clean, writer = preprocess.process(
                    chunk,
                    writer,
                )

                transformed_df = transformer.transform(clean)

                # Load the transformed chunk
                chunk_inserted = loader.load(transformed_df)
                total_inserted += chunk_inserted

                logger.info("Construction Plan Types Job", "Load", f"Loaded {chunk_inserted} rows from chunk {i}")

    except (ValueError, pd.errors.ParserError) as e:
        # These are data quality issues that should not be retried
        error_detail = f"{type(e).__name__}: {str(e)}"
        logger.error("Construction Plan Types Job", "Extract", f"Data quality error: {error_detail}")
        _move_source_file_to_error(source_file, "DATA_QUALITY", error_detail)
        # Re-raise so the task is marked as failed in Airflow
        raise
    except Exception as e:
        # For other exceptions (database errors, network issues, etc.),
        # let them bubble up so Airflow can handle retries
        # Don't move the source file as the issue might be transient
        logger.error("Construction Plan Types Job", "Load", f"Transient error: {type(e).__name__}: {str(e)}")
        raise
    finally:
        if writer is not None:
            writer.close()

    # Archive the staging file after all processing is done
    if writer is not None:
        logger.info("Construction Plan Types Job", "Load", f"Archiving staging files from {loader.staging_dir}")
        loader.archive(loader.staging_dir)

    logger.info("Construction Plan Types Job", "Complete", f"ETL job completed successfully. Total rows inserted: {total_inserted}")
    print(f"{total_inserted} rows inserted.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="ETL Job for Construction Plan Types"
    )

    parser.add_argument(
        "--chunksize",
        type=int,
        default=None,
        help=(
            "Number of rows to process per chunk. "
            "If omitted, the entire file is processed at once."
        ),
    )

    parser.add_argument(
        "--dryrun",
        type=str,
        default=None,
        help=(
            "Set to 'true', 'yes', 'on', or '1' to use the test schema (data_test). "
            "Any other value (including unset) uses the lake schema (data_lake)."
        ),
    )

    args = parser.parse_args()

    main(chunksize=args.chunksize, dryrun=args.dryrun)