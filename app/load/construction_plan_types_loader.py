import os
from pathlib import Path
import shutil

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


class ConstructionPlanTypesLoader:

    TABLE_NAME = "construction_plan_types"

    def __init__(self):

        warehouse_url = os.getenv("WAREHOUSE_URL")

        if warehouse_url:
            self.warehouse_url = warehouse_url
            self.schema = os.getenv("SCHEMA", "data_test")
        else:
            self.warehouse_url = (
                f"postgresql://"
                f"{os.getenv('POSTGRES_DB_SCHEMA_DATA_LAKE_USER')}:"
                f"{os.getenv('POSTGRES_DB_SCHEMA_DATA_LAKE_PASSWORD')}@"
                f"{os.getenv('POSTGRES_DB_SCHEMA_DATA_LAKE_HOST')}:"
                f"{os.getenv('POSTGRES_DB_SCHEMA_DATA_LAKE_PORT')}/"
                f"{os.getenv('POSTGRES_DB_SCHEMA_DATA_LAKE_NAME')}"
            )

            self.schema = os.getenv(
                "POSTGRES_DB_SCHEMA_DATA_LAKE_SCHEMA",
                "data_test",
            )

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