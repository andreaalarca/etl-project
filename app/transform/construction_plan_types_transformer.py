import os
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


class ConstructionPlanTypesTransformer:

    def __init__(self):

        # self.staging_dir = Path(
        #     os.getenv("STAGING_DIR", "./data/staging")
        # )

        # Source -> SQL mapping
        self.COLUMN_MAPPING = {
            "plan_type_name": "plan_type",
            "plan_type_id": "plan_type_id",
            "plan_category": "plan_category",
            "required_input": "required_input",
            "output_format": "output_format",
            "complexity_level": "complexity_level",
            "base_price": "base_price",
        }

        # SQL schema
        self.SQL_COLUMNS = [
            "plan_type_id",
            "plan_type",
            "plan_category",
            "required_input",
            "output_format",
            "complexity_level",
            "base_price",
        ]

        self.DEFAULT_VALUES = {
            "plan_category": None,
            "required_input": None,
            "output_format": None,
            "complexity_level": None,
            "base_price": 0.0,
        }

        self.SQL_DTYPES = {
            "plan_type_id": "Int64",
            "plan_type": "string",
            "plan_category": "string",
            "required_input": "string",
            "output_format": "string",
            "complexity_level": "string",
            "base_price": "float64",
        }

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df.copy()

        # ------------------------------------
        # Map source headers
        # ------------------------------------

        df.rename(
            columns=self.COLUMN_MAPPING,
            inplace=True,
        )

        # ------------------------------------
        # Validate SQL columns
        # ------------------------------------

        required = [
            "plan_type_id",
            "plan_type",
        ]

        missing = [
            c
            for c in required
            if c not in df.columns
        ]

        if missing:
            raise ValueError(
                f"Missing SQL columns: {missing}"
            )

        # ------------------------------------
        # Add missing destination columns
        # ------------------------------------

        for column in self.SQL_COLUMNS:

            if column not in df.columns:

                df[column] = self.DEFAULT_VALUES.get(column)

        # ------------------------------------
        # Remove unnecessary columns
        # ------------------------------------

        df = df[self.SQL_COLUMNS]

        # ------------------------------------
        # Convert SQL datatypes
        # ------------------------------------

        df["plan_type_id"] = (
            pd.to_numeric(
                df["plan_type_id"],
                errors="coerce",
            )
            .astype("Int64")
        )

        df["base_price"] = (
            pd.to_numeric(
                df["base_price"],
                errors="coerce",
            )
            .fillna(0)
            .astype("float64")
        )

        for col in [
            "plan_type",
            "plan_category",
            "required_input",
            "output_format",
            "complexity_level",
        ]:

            df[col] = (
                df[col]
                .fillna("")
                .astype("string")
            )

        # ------------------------------------
        # Business rules
        # ------------------------------------

        df = df.dropna(
            subset=["plan_type_id"]
        )

        df.loc[
            df["base_price"] < 0,
            "base_price",
        ] = 0

        # ------------------------------------
        # Derived columns
        # ------------------------------------

        # Example
        # df["created_at"] = pd.Timestamp.now()

        return df
    
#     def write_parquet(
#         self,
#         df: pd.DataFrame,
#         output_path: Path,
#     ):

#         output_path.parent.mkdir(
#             parents=True,
#             exist_ok=True,
#         )

#         pq.write_table(
#             pa.Table.from_pandas(df),
#             output_path,
#             compression="snappy",
#     )
        
#     def process(
#         self,
#         input_parquet: Path,
#         output_parquet: Path,
#     ):

#         df = pd.read_parquet(input_parquet)

#         transformed_df = self.transform(df)

#         self.write_parquet(
#             transformed_df,
#             output_parquet,
#         )

#         return transformed_df
    
# if __name__ == "__main__":

#     transformer = ConstructionPlanTypesTransformer()

#     input_file = (
#         transformer.staging_dir
#         / "construction_plan_types_20260712.parquet"
#     )

#     output_file = (
#         transformer.staging_dir
#         / "construction_plan_types_transformed.parquet"
#     )

#     transformed_df = transformer.process(
#         input_parquet=input_file,
#         output_parquet=output_file,
#     )

#     print(transformed_df.head())