"""
Warehouse configuration for database connection settings.
"""

import os


class WarehouseConfig:
    """
    Configuration for the data warehouse connection.
    Reads environment variables to construct the database URL and schema.
    """

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