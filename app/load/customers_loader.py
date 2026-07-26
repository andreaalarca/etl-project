import os
from pathlib import Path
import shutil
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras

from app.config.warehouse_config import WarehouseConfig
from app.utils.logger import logger

load_dotenv()


class CustomersLoader:

    TABLE_NAME = "customers"

    def __init__(self):
        # Use WarehouseConfig to handle database connection details
        warehouse_config = WarehouseConfig()
        self.warehouse_url = warehouse_config.warehouse_url
        # CustomersLoader uses data_lake schema as specified
        self.schema = warehouse_config.get_schema("data_lake")

        self.batch_size = int(
            os.getenv("BATCH_SIZE", "1000")
        )

        self.engine = create_engine(
            self.warehouse_url,
            future=True,
        )

        # Ensure schema and table exist
        self._ensure_schema_and_table()

        base_archive_dir = Path(
            os.getenv(
                "ARCHIVE_DIR",
                "./data/archive/customers",
            )
        )
        # Create a timestamped subdirectory for this run to avoid overwriting files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.archive_dir = base_archive_dir / timestamp
        # Create the archive directory
        self.archive_dir.mkdir(parents=True, exist_ok=True)

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
                'customer_id INTEGER PRIMARY KEY',
                'customer_name VARCHAR(150) NOT NULL',
                'contact_person VARCHAR(100) NOT NULL',
                'phone_number VARCHAR(20) NOT NULL',
                'email VARCHAR(150) NOT NULL',
                'city VARCHAR(100) NOT NULL',
                'subcontractor_category VARCHAR(50) NOT NULL',
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
                        self._connection_commit(raw_connection)
            finally:
                if 'raw_connection' in locals():
                    raw_connection.close()
        except Exception as e:
            # Fallback to SQLAlchemy method if psycopg2 fails
            logger.warning("Customers Load", "Load", f"PostgreSQL bulk insert failed ({e}), falling back to SQLAlchemy")
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

    def _connection_commit(self, conn):
        """Commit the given connection."""
        conn.commit()

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


def execute() -> str:
    """
    Execute the customer data loading process.
    This method orchestrates the load step:
    1. Read transformed data from staging area
    2. Load data into the PostgreSQL warehouse
    3. Archive the staging file after successful load

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("Customers Load", "Start", "Beginning customer data loading into warehouse")

        # Import datetime here to avoid circular imports
        from datetime import datetime

        # Step 1: Locate input file from transformation phase
        today = datetime.now().strftime("%Y%m%d")
        input_file = Path(os.getenv('STAGING_DIR', './data/staging')) / f"customers_{today}.parquet"

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}. Ensure transformation step has completed.")

        # Step 2: Read data from staging area
        df = pd.read_parquet(input_file)
        record_count = len(df)
        logger.info("Customers Load", "Read Complete", f"Read {record_count} records from {input_file}")

        if df.empty:
            logger.warning("Customers Load", "Empty Input", "Received empty DataFrame from transformation")
            # Archive empty file to maintain pipeline consistency
            loader = CustomersLoader()
            loader.archive(input_file)
            return f"SUCCESS: Loaded 0 customer records into warehouse (empty input)"

        # Step 3: Load data to warehouse
        loader = CustomersLoader()
        rows_loaded = loader.load(df)
        logger.info("Customers Load", "Load Complete", f"Successfully loaded {rows_loaded} records into warehouse")

        # Step 4: Archive the staging file after successful load
        archive_path = loader.archive(input_file)
        logger.info("Customers Load", "Archive Complete", f"Archived staging file to {archive_path}")

        # Verify we loaded all records
        if rows_loaded != record_count:
            logger.warning("Customers Load", "Count Mismatch",
                          f"Expected to load {record_count} records but actually loaded {rows_loaded}")

        logger.info("Customers Load", "Success",
                   f"Customer data loading completed successfully. Loaded {rows_loaded} records into warehouse.")
        return f"SUCCESS: Loaded {rows_loaded} customer records into warehouse"

    except Exception as e:
        logger.error("Customers Load", "Error", f"Customer data loading failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed