"""
Warehouse configuration for database connection settings.
"""

import os
import re


class WarehouseConfig:
    """
    Configuration for the data warehouse connection.
    Provides connection URL for a selected dataset and discovers all available schemas.
    Makes infrastructural decisions (which dataset to connect to) but not domain decisions (which schema to use).
    """

    def __init__(self):
        # Determine which dataset configuration to use (infrastructural decision)
        dataset_type = os.getenv("DATASET_TYPE", "DATA_LAKE").upper()
        dry_run = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes", "on")

        print(f"DEBUG WarehouseConfig: DATASET_TYPE={dataset_type}, DRY_RUN env var={os.getenv('DRY_RUN', 'NOT_SET')}, dry_run={dry_run}")  # DEBUG

        # Build connection configuration based on dataset type and dry run flag
        if dry_run:
            prefix = f"TEST_POSTGRES_DB_SCHEMA_{dataset_type}"
            print(f"DEBUG WarehouseConfig: DRY RUN MODE - Using TEST_{dataset_type}_* env vars:")  # DEBUG
        else:
            prefix = f"POSTGRES_DB_SCHEMA_{dataset_type}"
            print(f"DEBUG WarehouseConfig: PRODUCTION MODE - Using POSTGRES_{dataset_type}_* env vars:")  # DEBUG

        host = os.getenv(f"{prefix}_HOST", "localhost")
        port = os.getenv(f"{prefix}_PORT", "5432")
        name = os.getenv(f"{prefix}_NAME", "analytics")
        user = os.getenv(f"{prefix}_USER", "postgres")
        password = os.getenv(f"{prefix}_PASSWORD", "")

        print(f"  Host: {host}")  # DEBUG
        print(f"  Port: {port}")  # DEBUG
        print(f"  Database: {name}")  # DEBUG
        print(f"  User: {user}")  # DEBUG

        # Construct the warehouse URL
        self.warehouse_url = (
            f"postgresql://{user}:{password}@{host}:{port}/{name}"
        )  # DEBUG
        print(f"DEBUG WarehouseConfig: Constructed URL: {self.warehouse_url}")  # DEBUG

        # Provide a dictionary of all available schemas - consumers (loaders) choose which to use
        # Automatically discovers schema environment variables in two formats:
        # 1. [TEST_]POSTGRES_DB_SCHEMA_<DATASET>_SCHEMA=<schema_value>
        # 2. [TEST_]POSTGRES_SCHEMA_<DATASET>=<schema_value>
        self.schemas = {}
        pattern1 = re.compile(r'(?:TEST_)?POSTGRES_DB_SCHEMA_([A-Z_]+)_SCHEMA')
        pattern2 = re.compile(r'(?:TEST_)?POSTGRES_SCHEMA_([A-Z_]+)')

        for key, value in os.environ.items():
            match = pattern1.match(key)
            if match:
                # Format 1: [TEST_]POSTGRES_DB_SCHEMA_<DATASET>_SCHEMA
                dataset_name = match.group(1)
                key_lower = dataset_name.lower()
                self.schemas[key_lower] = value
                print(f"  Discovered schema (format 1): {dataset_name} = {value}")  # DEBUG
                continue

            match = pattern2.match(key)
            if match:
                # Format 2: [TEST_]POSTGRES_SCHEMA_<DATASET>
                dataset_name = match.group(1)
                key_lower = dataset_name.lower()
                self.schemas[key_lower] = value
                print(f"  Discovered schema (format 2): {dataset_name} = {value}")  # DEBUG
                continue

        print(f"DEBUG WarehouseConfig: schemas={self.schemas}")  # DEBUG
        print(f"DEBUG: WarehouseConfig initialized for dataset_type={dataset_type}")  # DEBUG

    def get_schema(self, name: str) -> str:
        """
        Retrieve schema name by identifier.

        Args:
            name: Schema identifier (case-insensitive)

        Returns:
            The actual schema name to use in database

        Raises:
            KeyError: If schema identifier not found
        """
        name_lower = name.lower()
        if name_lower not in self.schemas:
            raise KeyError(
                f"Schema '{name}' not found. "
                f"Available schemas: {sorted(self.schemas.keys())}"
            )
        return self.schemas[name_lower]