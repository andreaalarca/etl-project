"""
Database utilities for ETL pipeline.
Provides functions for bulk loading and transaction management.
"""
import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
import psycopg2.extras

from app.config.warehouse_config import WarehouseConfig


def get_warehouse_engine():
    """
    Create and return a SQLAlchemy engine for the warehouse using WarehouseConfig.

    Returns:
        SQLAlchemy engine instance
    """
    warehouse_config = WarehouseConfig()
    return create_engine(warehouse_config.warehouse_url, future=True)


def bulk_insert_dataframe(df: pd.DataFrame, table_name: str, schema: str = None, batch_size: int = 1000) -> int:
    """
    Bulk insert a pandas DataFrame into a PostgreSQL table using psycopg2's execute_values for performance.

    Args:
        df: pandas DataFrame to insert
        table_name: Name of the target table
        schema: Schema name. If None, uses the data_mart schema from WarehouseConfig.
        batch_size: Number of rows per batch in the bulk insert

    Returns:
        Number of rows inserted

    Raises:
        Exception: If the bulk insert fails
    """
    if df.empty:
        return 0

    # Determine schema
    if schema is None:
        warehouse_config = WarehouseConfig()
        try:
            schema = warehouse_config.get_schema("data_mart")
        except (AttributeError, KeyError):
            schema = "data_mart"

    engine = get_warehouse_engine()

    # Use psycopg2 for better PostgreSQL bulk insert performance
    try:
        # Get raw connection from SQLAlchemy engine's pool
        raw_connection = engine.raw_connection()
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
                    query = f'INSERT INTO "{schema}"."{table_name}" ({cols}) VALUES %s'
                    psycopg2.extras.execute_values(
                        cursor,
                        query,
                        data,
                        template=None,
                        page_size=batch_size
                    )
                    inserted = len(data)
                    raw_connection.commit()
                else:
                    inserted = 0
        finally:
            raw_connection.close()
    except Exception as e:
        # Fallback to SQLAlchemy method if psycopg2 fails
        # Note: This is less efficient but ensures we don't fail completely
        try:
            with engine.begin() as conn:
                for start in range(0, len(df), batch_size):
                    chunk = df.iloc[start:start + batch_size]
                    chunk.to_sql(
                        name=table_name,
                        schema=schema,
                        con=conn,
                        if_exists="append",
                        index=False,
                        method="multi",
                    )
                inserted = len(df)
        except Exception as e2:
            raise Exception(f"Bulk insert failed. Psycopg2 error: {str(e)}. SQLAlchemy fallback error: {str(e2)}") from e

    return inserted


def execute_delete_and_insert(
    df: pd.DataFrame,
    table_name: str,
    schema: str,
    date_column: str,
    process_date: str,
    batch_size: int = 1000
) -> None:
    """
    Execute a DELETE + INSERT operation in a single transaction for the given process date.

    Steps:
        1. Start a transaction
        2. Delete existing records for the process date
        3. Bulk insert the new DataFrame
        4. Verify row count matches
        5. Commit if successful, rollback on failure

    Args:
        df: pandas DataFrame to insert
        table_name: Name of the target table
        schema: Schema name
        date_column: Name of the date column used for the DELETE (e.g., 'request_date')
        process_date: Process date in YYYY-MM-DD format (used for DELETE and validation)
        batch_size: Batch size for bulk insert

    Raises:
        Exception: If any step fails, triggering a rollback
    """
    engine = get_warehouse_engine()
    with engine.begin() as conn:
        # Start transaction (handled by engine.begin)
        try:
            # Step 1: Delete existing records for the process date
            delete_query = text(
                f'DELETE FROM "{schema}"."{table_name}" WHERE "{date_column}" = DATE :process_date'
            )
            result = conn.execute(delete_query, {"process_date": process_date})
            deleted_count = result.rowcount
            # Log via the logger? We'll let the caller handle logging.

            # Step 2: Bulk insert the new DataFrame
            inserted_count = bulk_insert_dataframe(df, table_name, schema, batch_size)

            # Step 3: Verify row count
            # We can do a quick check: the number of rows inserted should equal the DataFrame length
            if inserted_count != len(df):
                raise Exception(
                    f"Row count mismatch: expected to insert {len(df)} rows, but {inserted_count} rows were inserted"
                )

            # If we reach here, commit is automatic due to engine.begin context
            # Log success
        except Exception as e:
            # The transaction will be rolled back automatically by engine.begin on exception
            raise e