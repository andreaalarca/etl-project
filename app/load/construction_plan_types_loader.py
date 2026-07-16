import os
from pathlib import Path
import shutil

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime
import psycopg2
import psycopg2.extras

from app.config.warehouse_config import WarehouseConfig

load_dotenv()


class ConstructionPlanTypesLoader:

    TABLE_NAME = "construction_plan_types"

    def __init__(self):
        # Use WarehouseConfig to handle database connection details
        warehouse_config = WarehouseConfig()
        self.warehouse_url = warehouse_config.warehouse_url
        # Get the specific schema this loader needs - ConstructionPlanTypesLoader uses data_lake
        self.schema = warehouse_config.get_schema("data_lake")
        print(f"DEBUG: WarehouseConfig initialized with schema: {self.schema}")  # DEBUG

        self.batch_size = int(
            os.getenv("BATCH_SIZE", "1000")
        )

        self.engine = create_engine(
            self.warehouse_url,
            future=True,
        )

        base_archive_dir = Path(
            os.getenv(
                "ARCHIVE_DIR",
                "./data/archive/construction_plan_types",
            )
        )
        # Create a timestamped subdirectory for this run to avoid overwriting files
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.archive_dir = base_archive_dir / timestamp

        today = datetime.now().strftime("%Y%m%d")

        self.staging_dir = Path(
            f"data/staging/construction_plan_types_{today}.parquet"
        )

        self.archive_dir.mkdir(
            parents=True,
            exist_ok=True,
        )


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

        shutil.move(
            str(source_file),
            str(destination),
        )

        return destination

    # def process(
    #     self,
    #     parquet_file: Path,
    # ) -> int:
    #     """
    #     Read transformed parquet,
    #     load into SQL,
    #     then archive the parquet.
    #     """

    #     df = pd.read_parquet(parquet_file)

    #     inserted = self.load(df)

    #     self.archive(parquet_file)

    #     return inserted


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