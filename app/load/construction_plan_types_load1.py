#!/usr/bin/env python3
"""
Load construction_plan_types Parquet into warehouse and archive the source.
"""

import os
import shutil
from pathlib import Path
import logging

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

def build_warehouse_url(dry_run: bool = False):
    """Build warehouse URL and schema based on mode and environment variables."""
    # 1. Check for legacy WAREHOUSE_URL override (highest priority)
    warehouse_url = os.getenv("WAREHOUSE_URL")
    if warehouse_url:
        schema = os.getenv("SCHEMA", "data_test")
        return warehouse_url, schema

    # 2. Determine which config set to use: TEST (if dry-run flag) or DATA_LAKE
    if dry_run:
        # Use TEST_* variables
        host = os.getenv("TEST_POSTGRES_DB_SCHEMA_DATA_LAKE_HOST")
        port = os.getenv("TEST_POSTGRES_DB_SCHEMA_DATA_LAKE_PORT")
        database = os.getenv("TEST_POSTGRES_DB_SCHEMA_DATA_LAKE_NAME")
        user = os.getenv("TEST_POSTGRES_DB_SCHEMA_DATA_LAKE_USER")
        password = os.getenv("TEST_POSTGRES_DB_SCHEMA_DATA_LAKE_PASSWORD")
        schema = os.getenv("TEST_POSTGRES_DB_SCHEMA_DATA_LAKE_SCHEMA", "data_test")
        config_name = "TEST"
    else:
        # Use regular DATA_LAKE variables
        host = os.getenv("POSTGRES_DB_SCHEMA_DATA_LAKE_HOST")
        port = os.getenv("POSTGRES_DB_SCHEMA_DATA_LAKE_PORT")
        database = os.getenv("POSTGRES_DB_SCHEMA_DATA_LAKE_NAME")
        user = os.getenv("POSTGRES_DB_SCHEMA_DATA_LAKE_USER")
        password = os.getenv("POSTGRES_DB_SCHEMA_DATA_LAKE_PASSWORD")
        schema = os.getenv("POSTGRES_DB_SCHEMA_DATA_LAKE_SCHEMA", "data_test")
        config_name = "DATA_LAKE"

    # Validate required variables are present
    missing = []
    if not host:
        missing.append(f"{config_name}_POSTGRES_DB_SCHEMA_DATA_LAKE_HOST")
    if not port:
        missing.append(f"{config_name}_POSTGRES_DB_SCHEMA_DATA_LAKE_PORT")
    if not database:
        missing.append(f"{config_name}_POSTGRES_DB_SCHEMA_DATA_LAKE_NAME")
    if not user:
        missing.append(f"{config_name}_POSTGRES_DB_SCHEMA_DATA_LAKE_USER")

    if missing:
        raise RuntimeError(
            f"Warehouse URL not configured. Either set WAREHOUSE_URL, "
            f"or complete {config_name} PostgreSQL variables. "
            f"Missing: {', '.join(missing)}"
        )

    if password:
        warehouse_url = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    else:
        warehouse_url = f"postgresql://{user}@{host}:{port}/{database}"
    return warehouse_url, schema

# Get dry-run mode from environment variable (default False)
DRY_RUN = os.getenv("DRY_RUN", "false").lower() == "true"
warehouse_url, schema = build_warehouse_url(DRY_RUN)

STAGING_DIR = os.getenv("STAGING_DIR", "./data/staging")
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", "./data/archive/construction_plan_types")
TABLE_NAME = "construction_plan_types"

if not warehouse_url:
    raise RuntimeError("Warehouse URL not configured.")

def _latest_parquet() -> Path:
    """Find the most recent construction_plan_types parquet file in staging directory."""
    staging_path = Path(STAGING_DIR)
    pattern = "construction_plan_types_*.parquet"
    parquet_files = list(staging_path.glob(pattern))

    if not parquet_files:
        raise FileNotFoundError(f"No construction_plan_types parquet files found in {staging_path} matching {pattern}")

    # Sort by modification time (most recent first)
    latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)
    return latest_file

def _validate(df: pd.DataFrame) -> pd.DataFrame:
    expected = ["plan_type_id", "plan_type_name", "description", "base_price", "is_active"]
    missing = set(expected) - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    df = df[expected].copy()
    df["plan_type_id"] = pd.to_numeric(df["plan_type_id"], errors="coerce").astype("Int64")
    df["base_price"] = pd.to_numeric(df["base_price"], errors="coerce").astype("float64")
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip()
    df = df.dropna(subset=["plan_type_id"])
    return df

def main(dry_run: bool = None) -> bool:
    """
    Main function to load construction_plan_types Parquet into warehouse.

    Args:
        dry_run: If True, use test database configuration. If None, uses DRY_RUN env var.

    Returns:
        bool: True if successful, False otherwise
    """
    # Determine effective dry_run mode
    effective_dry_run = DRY_RUN if dry_run is None else dry_run
    # Build warehouse URL and schema for the effective mode
    warehouse_url_eff, schema_eff = build_warehouse_url(effective_dry_run)

    try:
        src_path = _latest_parquet()
        logger.info(f"Reading {src_path}")
        df = pd.read_parquet(src_path)
        logger.info(f"Loaded {len(df)} rows")
        df = _validate(df)
        logger.info(f"After validation: {len(df)} rows")
        if df.empty:
            logger.warning("No valid data to load")
        else:
            engine = create_engine(warehouse_url_eff, future=True)
            batch_size = int(os.getenv("BATCH_SIZE", "1000"))
            total = len(df)
            logger.info(f"Inserting in batches of {batch_size} rows")

            # Create error directory for failed chunks
            error_dir = os.getenv("ERROR_DIR", "./data/error")
            Path(error_dir).mkdir(parents=True, exist_ok=True)

            with engine.begin() as conn:
                for start in range(0, total, batch_size):
                    end = start + batch_size
                    chunk = df.iloc[start:end]
                    try:
                        chunk.to_sql(
                            name=TABLE_NAME,
                            schema=schema_eff,
                            con=conn,
                            if_exists="append",
                            index=False,
                            method="multi",
                        )
                        logger.info(f"Inserted rows {start + 1}-{min(end, total)} of {total}")
                    except Exception as chunk_error:
                        # Save the failed chunk to CSV for debugging
                        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                        error_file = Path(error_dir) / f"construction_plan_types_failed_chunk_{start}_{end}_{timestamp}.csv"
                        chunk.to_csv(error_file, index=False)
                        logger.error(f"Failed to insert chunk {start+1}-{min(end, total)}. Saved to {error_file}")
                        logger.error(f"Chunk error: {chunk_error}")
                        # Continue with next chunk rather than failing entire process
                        continue

            logger.info(f"Inserted {total} rows into {schema_eff}.{TABLE_NAME}")
        # Archive
        archive_dir = Path(ARCHIVE_DIR)
        archive_dir.mkdir(parents=True, exist_ok=True)
        ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        dst = archive_dir / f"construction_plan_types_{ts}.parquet"
        shutil.move(str(src_path), str(dst))
        logger.info(f"Archived to {dst}")
        return True
    except Exception as e:
        logger.error(f"{e}")
        return False

if __name__ == "__main__":
    exit(0 if main() else 1)