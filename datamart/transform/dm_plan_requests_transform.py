import os
import pandas as pd
import logging
from dotenv import load_dotenv

from app.utils.logger import logger
from app.config.warehouse_config import WarehouseConfig
from sqlalchemy import create_engine, text

load_dotenv()


class DMPLANREQUESTSTransformer:

    def __init__(self):
        # Initialize database connection
        warehouse_config = WarehouseConfig()
        self.warehouse_url = warehouse_config.warehouse_url
        self.engine = create_engine(self.warehouse_url, future=True)
        logger.info("DM Plan Requests Job", "Transform", "Initialized transformer for data mart join")

    def transform(self) -> pd.DataFrame:
        """
        Transform data by joining plan_requests, customers, and construction_plan_types from data_lake schema.

        Returns:
            pandas DataFrame with the joined data.
        """
        logger.info("DM Plan Requests Job", "Transform", "Starting data transformation with JOIN")

        # SQL query as provided by the user
        query = text("""
            SELECT
                pr.request_id,
                c.customer_id,
                c.customer_name,
                c.contact_person,
                c.phone_number,
                c.email,
                c.city,
                c.subcontractor_category,

                pt.plan_type_id,
                pt.plan_type,
                pt.plan_category,
                pt.required_input,
                pt.output_format,
                pt.complexity_level,
                pt.base_price,

                pr.request_date,
                pr.target_date,
                pr.completed_date,
                pr.status,
                pr.priority,
                pr.assigned_coordinator,
                pr.floor_area_sqm,
                pr.revision_count

            FROM data_lake.plan_requests pr
            INNER JOIN data_lake.customers c
                ON pr.customer_id = c.customer_id
            INNER JOIN data_lake.construction_plan_types pt
                ON pr.plan_type_id = pt.plan_type_id
        """)

        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn)
                logger.info("DM Plan Requests Job", "Transform", f"Retrieved {len(df)} rows from joined tables")
        except Exception as e:
            logger.error("DM Plan Requests Job", "Transform", f"Failed to execute join query: {str(e)}")
            raise

        if df.empty:
            logger.warning("DM Plan Requests Job", "Transform", "Received empty DataFrame from join")
            return df

        # Apply final data type conversions to match the target table schema
        # ID columns should be integers
        id_columns = ['request_id', 'customer_id', 'plan_type_id']
        for col in id_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # String columns
        string_columns = [
            'customer_name', 'contact_person', 'phone_number', 'email',
            'city', 'subcontractor_category', 'plan_type', 'plan_category',
            'required_input', 'output_format', 'complexity_level',
            'status', 'priority', 'assigned_coordinator'
        ]
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].fillna('').astype('string')

        # Date columns
        date_columns = ['request_date', 'target_date', 'completed_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Numeric columns
        numeric_columns = ['floor_area_sqm', 'base_price']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('float64')

        # Revision count should be integer
        if 'revision_count' in df.columns:
            df['revision_count'] = pd.to_numeric(df['revision_count'], errors='coerce').fillna(0).astype('int64')

        logger.info("DM Plan Requests Job", "Transform", f"Transformation complete. Final shape: {df.shape}")
        return df