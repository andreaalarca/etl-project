import os
from pathlib import Path
import shutil

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
import psycopg2.extras

from app.config.warehouse_config import WarehouseConfig
from app.utils.logger import logger

load_dotenv()


class DMPLANREQUESTSLoader:

    TABLE_NAME = "dm_plan_requests"

    def __init__(self):
        # Use WarehouseConfig to handle database connection details
        warehouse_config = WarehouseConfig()
        self.warehouse_url = warehouse_config.warehouse_url
        # Try to get schema for data_mart, fallback to "data_mart"
        try:
            self.schema = warehouse_config.get_schema("data_mart")
        except (AttributeError, KeyError):
            self.schema = "data_mart"
        print(f"DEBUG: WarehouseConfig initialized with schema: {self.schema}")  # DEBUG

        self.batch_size = int(
            os.getenv("BATCH_SIZE", "1000")
        )

        self.engine = create_engine(
            self.warehouse_url,
            future=True,
        )

        # Ensure schema and table exist
        self._ensure_schema_and_table()

        today = datetime.now().strftime("%Y%m%d")
        self.staging_dir = Path(
            f"data/staging/dm_plan_requests_{today}.parquet"
        )

        base_archive_dir = Path(
            os.getenv(
                "ARCHIVE_DIR",
                "./data/archive/dm_plan_requests",
            )
        )
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.archive_dir = base_archive_dir / timestamp

        self.archive_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

    def _ensure_schema_and_table(self):
        """Create the schema and table if they do not already exist."""
        inspector = inspect(self.engine)
        schema_name = self.schema
        table_name = self.TABLE_NAME

        # Create schema if missing
        if not inspector.has_schema(schema_name):
            with self.engine.begin() as conn:
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}"'))
            # Update inspector to see the new schema
            inspector = inspect(self.engine)

        # Drop table if it exists (for development/testing to ensure correct schema)
        if inspector.has_table(table_name, schema=schema_name):
            with self.engine.begin() as conn:
                conn.execute(text(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE'))
            # Update inspector after dropping
            inspector = inspect(self.engine)

        # Create table if missing
        if not inspector.has_table(table_name, schema=schema_name):
            # Define columns matching the specification
            columns = [
                'request_id INTEGER PRIMARY KEY',
                'customer_id INTEGER NOT NULL',
                'customer_name VARCHAR(150) NOT NULL',
                'contact_person VARCHAR(100) NOT NULL',
                'phone_number VARCHAR(20) NOT NULL',
                'email VARCHAR(150) NOT NULL',
                'city VARCHAR(100) NOT NULL',
                'subcontractor_category VARCHAR(50) NOT NULL',
                'plan_type_id INTEGER NOT NULL',
                'plan_type VARCHAR(100) NOT NULL',
                'plan_category VARCHAR(50) NOT NULL',
                'required_input TEXT NOT NULL',
                'output_format VARCHAR(50) NOT NULL',
                'complexity_level VARCHAR(20) NOT NULL',
                'base_price NUMERIC(10,2) NOT NULL',
                'request_date DATE NOT NULL',
                'target_date DATE',
                'completed_date DATE',
                'status VARCHAR(30) NOT NULL',
                'priority VARCHAR(20) NOT NULL',
                'assigned_coordinator VARCHAR(100) NOT NULL',
                'floor_area_sqm NUMERIC(10,2) NOT NULL',
                'revision_count INTEGER NOT NULL',
            ]

            cols_sql = ',\n    '.join(columns)
            create_sql = f'''
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                {cols_sql}
            );
            '''

            with self.engine.begin() as conn:
                conn.execute(text(create_sql))

    def load(
        self,
        df: pd.DataFrame,
    ) -> int:
        """
        Load SQL-ready DataFrame.

        Returns
        -------
        int
            Number of inserted rows.
        """

        if df.empty:
            return 0

        inserted = 0

        # Use psycopg2 for better PostgreSQL bulk insert performance
        try:
            # Get raw connection from SQLAlchemy engine's pool
            raw_connection = self.engine.raw_connection()
            try:
                with raw_connection.cursor() as cursor:
                    # Prepare data for bulk insert
                    # Convert DataFrame to list of tuples, handling NaN values and numpy types
                    def convert_val(val):
                        if pd.isna(val):
                            return None
                        elif hasattr(val, 'item'):  # numpy scalar
                            return val.item()
                        else:
                            return val

                    data = [tuple(convert_val(val) for val in row)
                           for row in df.itertuples(index=False, name=None)]

                    if data:
                        # Use execute_values for efficient bulk insert
                        cols = ','.join([f'"{col}"' for col in df.columns])
                        query = f'INSERT INTO "{self.schema}"."{self.TABLE_NAME}" ({cols}) VALUES %s'
                        psycopg2.extras.execute_values(
                            cursor,
                            query,
                            data,
                            template=None,
                            page_size=self.batch_size
                        )
                        inserted = len(data)
                        raw_connection.commit()
            finally:
                raw_connection.close()
        except Exception as e:
            # Fallback to SQLAlchemy method if psycopg2 fails
            print(f"Warning: psycopg2 bulk insert failed ({e}), falling back to SQLAlchemy")
            with self.engine.begin() as conn:
                for start in range(
                    0,
                    len(df),
                    self.batch_size,
                ):

                    chunk = df.iloc[
                        start:start + self.batch_size
                    ]

                    chunk.to_sql(
                        name=self.TABLE_NAME,
                        schema=self.schema,
                        con=conn,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )

                    inserted += len(chunk)

        return inserted

    def execute(self) -> str:
        """
        Execute the dm_plan_requests data loading process.
        This method orchestrates the load step:
        1. Read transformed data from staging area
        2. Load data into the PostgreSQL warehouse
        3. Archive the staging file after successful load

        Returns:
            str: Status message indicating success and details
        """
        try:
            logger.info("DM Plan Requests Load", "Start", "Beginning dm_plan_requests data loading into warehouse")

            # Import datetime here to avoid circular imports
            from datetime import datetime

            # Step 1: Locate input file from transformation phase
            today = datetime.now().strftime("%Y%m%d")
            input_file = Path(os.getenv('STAGING_DIR', './data/staging')) / f"dm_plan_requests_{today}.parquet"

            if not input_file.exists():
                raise FileNotFoundError(f"Input file not found: {input_file}. Ensure transformation step has completed.")

            # Step 2: Read data from staging area
            df = pd.read_parquet(input_file)
            record_count = len(df)
            logger.info("DM Plan Requests Load", "Read Complete", f"Read {record_count} records from {input_file}")

            if df.empty:
                logger.warning("DM Plan Requests Load", "Empty Input", "Received empty DataFrame from transformation")
                # Archive empty file to maintain pipeline consistency
                loader = DMPLANREQUESTSLoader()
                loader.archive(input_file)
                return f"SUCCESS: Loaded 0 dm_plan_requests records into warehouse (empty input)"

            # Step 3: Load data to warehouse
            loader = DMPLANREQUESTSLoader()
            rows_loaded = loader.load(df)
            logger.info("DM Plan Requests Load", "Load Complete", f"Successfully loaded {rows_loaded} records into warehouse")

            # Step 4: Archive the staging file after successful load
            archive_path = loader.archive(input_file)
            logger.info("DM Plan Requests Load", "Archive Complete", f"Archived staging file to {archive_path}")

            # Verify we loaded all records
            if rows_loaded != record_count:
                logger.warning("DM Plan Requests Load", "Count Mismatch",
                              f"Expected to load {record_count} records but actually loaded {rows_loaded}")

            logger.info("DM Plan Requests Load", "Success",
                       f"DM plan requests data loading completed successfully. Loaded {rows_loaded} records into warehouse.")
            return f"SUCCESS: Loaded {rows_loaded} dm_plan_requests records into warehouse"

        except Exception as e:
            logger.error("DM Plan Requests Load", "Error", f"DM plan requests data loading failed: {str(e)}")
            raise  # Re-raise so Airflow marks task as failed

    def archive(
        self,
        source_file: Path,
    ) -> Path:
        """
        Move successfully loaded parquet
        from staging to archive.
        """

        destination = (
            self.archive_dir /
            source_file.name
        )
        # Ensure the destination directory exists
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(
            str(source_file),
            str(destination),
        )

        return destination

    def load_parquet(
        self,
        parquet_file: Path,
    ) -> int:
        """
        Read transformed parquet then load.
        """

        df = pd.read_parquet(parquet_file)

        return self.load(df)

    def process(
        self,
        df: pd.DataFrame
    ) -> int:

        inserted = self.load(df)

        self.archive(self.staging_dir)

        return inserted