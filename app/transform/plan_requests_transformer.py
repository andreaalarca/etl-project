import os
from pathlib import Path
import pandas as pd
import logging
from datetime import datetime

from app.utils.logger import logger

class PlanRequestsTransformer:

    def __init__(self):
        # Load construction_plan_types mapping from CSV file
        self.plan_type_map = self._load_plan_type_mapping()
        logger.info("Plan Requests Job", "Transform", f"Loaded {len(self.plan_type_map)} plan types from CSV")

    def _load_plan_type_mapping(self) -> dict:
        """
        Load construction_plan_types from the source CSV file and return a dict
        mapping plan_type_id to a dict of attributes.
        """
        # Determine the path to the source data directory
        # Assuming the project root is two levels up from this file (app/transform -> project root)
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
        csv_path = os.path.join(base_dir, 'data', 'source', 'construction_plan_types.csv')

        try:
            df = pd.read_csv(csv_path)
            # Ensure plan_type_id is integer
            df['plan_type_id'] = pd.to_numeric(df['plan_type_id'], errors='coerce').astype('Int64')
            # Create mapping dictionary
            mapping = {}
            for _, row in df.iterrows():
                pt_id = row['plan_type_id']
                if pd.isna(pt_id):
                    continue
                mapping[int(pt_id)] = {
                    'plan_type': row['plan_type'],
                    'plan_category': row['plan_category'],
                    'required_input': row['required_input'],
                    'output_format': row['output_format'],
                    'complexity_level': row['complexity_level'],
                    'base_price': row['base_price']
                }
            return mapping
        except Exception as e:
            logger.error("Plan Requests Job", "Transform", f"Failed to load plan type mapping from {csv_path}: {str(e)}")
            # Return empty mapping; downstream will handle missing data
            return {}

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform plan requests data by mapping plan_type_id to plan type attributes.
        No database join; mapping is done in-memory from CSV.

        Expected input columns (after preprocessing):
        request_id, customer_id, plan_type_id, request_date, target_date,
        completed_date, status, priority, assigned_coordinator, floor_area_sqm, revision_count

        Output columns:
        request_id, customer_id, plan_type_id, plan_type, plan_category,
        required_input, output_format, complexity_level, base_price,
        request_date, target_date, completed_date, status, priority,
        assigned_coordinator, floor_area_sqm, revision_count
        """
        logger.info("Plan Requests Job", "Transform", "Starting data transformation with mapping from CSV")

        if df.empty:
            logger.warning("Plan Requests Job", "Transform", "Received empty DataFrame")
            return df

        df = df.copy()

        # Ensure plan_type_id is integer for mapping
        if 'plan_type_id' in df.columns:
            df['plan_type_id'] = pd.to_numeric(df['plan_type_id'], errors='coerce').astype('Int64')

        # Map plan_type_id to attributes
        def map_row(row):
            pt_id = row['plan_type_id']
            if pd.isna(pt_id) or pt_id not in self.plan_type_map:
                # Return None/NaN for missing mappings; will be filled later
                return pd.Series({
                    'plan_type': None,
                    'plan_category': None,
                    'required_input': None,
                    'output_format': None,
                    'complexity_level': None,
                    'base_price': None
                })
            mapping = self.plan_type_map[int(pt_id)]
            return pd.Series({
                'plan_type': mapping['plan_type'],
                'plan_category': mapping['plan_category'],
                'required_input': mapping['required_input'],
                'output_format': mapping['output_format'],
                'complexity_level': mapping['complexity_level'],
                'base_price': mapping['base_price']
            })

        # Apply mapping and join results
        mapped = df.apply(map_row, axis=1)
        df = pd.concat([df, mapped], axis=1)

        logger.info("Plan Requests Job", "Transform", f"After mapping: {len(df)} rows")

        # Select and order columns as per the original specification
        result_columns = [
            'request_id',
            'customer_id',
            'plan_type_id',
            'plan_type',
            'plan_category',
            'required_input',
            'output_format',
            'complexity_level',
            'base_price',
            'request_date',
            'target_date',
            'completed_date',
            'status',
            'priority',
            'assigned_coordinator',
            'floor_area_sqm',
            'revision_count'
        ]

        # Ensure we only select columns that exist
        available_columns = [col for col in result_columns if col in df.columns]
        result_df = df[available_columns].copy()

        # Sort by request_id as per specification
        if 'request_id' in result_df.columns:
            result_df = result_df.sort_values('request_id').reset_index(drop=True)

        # Apply final data type conversions
        # ID columns should be integers
        id_columns = ['request_id', 'customer_id', 'plan_type_id']
        for col in id_columns:
            if col in result_df.columns:
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce').astype('Int64')

        # String columns
        string_columns = ['plan_type', 'plan_category', 'required_input', 'output_format',
                         'complexity_level', 'status', 'priority', 'assigned_coordinator']
        for col in string_columns:
            if col in result_df.columns:
                result_df[col] = result_df[col].fillna('').astype('string')

        # Date columns
        date_columns = ['request_date', 'target_date', 'completed_date']
        for col in date_columns:
            if col in result_df.columns:
                result_df[col] = pd.to_datetime(result_df[col], errors='coerce')

        # Numeric columns
        numeric_columns = ['floor_area_sqm', 'base_price']
        for col in numeric_columns:
            if col in result_df.columns:
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce').fillna(0).astype('float64')

        # Revision count should be integer
        if 'revision_count' in result_df.columns:
            result_df['revision_count'] = pd.to_numeric(result_df['revision_count'], errors='coerce').fillna(0).astype('int64')

        logger.info("Plan Requests Job", "Transform", f"Transformation complete. Final shape: {result_df.shape}")
        return result_df


def execute() -> str:
    """
    Execute the plan requests data transformation process.
    This method orchestrates the transform step:
    1. Read preprocessed data from staging area
    2. Apply data type transformations
    3. Write transformed data back to staging area (overwriting input)

    Returns:
        str: Status message indicating success and details
    """
    try:
        logger.info("Plan Requests Transform", "Start", "Beginning plan requests data transformation")

        # Step 1: Locate input file from preprocessing phase
        today = datetime.now().strftime("%Y%m%d")
        input_file = Path(os.getenv('STAGING_DIR', './data/staging')) / f"plan_requests_{today}.parquet"

        if not input_file.exists():
            raise FileNotFoundError(f"Input file not found: {input_file}. Ensure preprocessing step has completed.")

        # Step 2: Read data from staging area
        df = pd.read_parquet(input_file)
        record_count = len(df)
        logger.info("Plan Requests Transform", "Read Complete", f"Read {record_count} records from {input_file}")

        if df.empty:
            logger.warning("Plan Requests Transform", "Empty Input", "Received empty DataFrame from preprocessing")
            # Still write empty DataFrame to maintain pipeline
            df.to_parquet(input_file, index=False)
            return f"SUCCESS: Transformed 0 plan requests records (empty input)"

        # Step 3: Transform data
        transformer = PlanRequestsTransformer()
        transformed_df = transformer.transform(df)

        # Verify we still have the same number of rows (transformation shouldn't filter rows)
        if len(transformed_df) != record_count:
            logger.warning("Plan Requests Transform", "Row Count Change",
                          f"Row count changed from {record_count} to {len(transformed_df)} during transformation")

        # Step 4: Write transformed data back to staging area (overwrite input)
        transformed_df.to_parquet(input_file, index=False)
        logger.info("Plan Requests Transform", "Write Complete",
                   f"Wrote {len(transformed_df)} transformed records to {input_file}")

        logger.info("Plan Requests Transform", "Success",
                   f"Plan requests transformation completed successfully. Transformed {len(transformed_df)} records.")
        return f"SUCCESS: Transformed {len(transformed_df)} plan requests records"

    except Exception as e:
        logger.error("Plan Requests Transform", "Error", f"Plan requests transformation failed: {str(e)}")
        raise  # Re-raise so Airflow marks task as failed