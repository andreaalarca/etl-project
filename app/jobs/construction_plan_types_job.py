#!/usr/bin/env python3
"""
ETL Job for Construction Plan Types data.
Orchestrates the extraction, transformation, and loading of construction plan types data.
Handles file acquisition, format detection, and archiving of source files.
"""

import argparse
import sys
from pathlib import Path

# Add the app directory to the path so we can import from app modules
sys.path.append(str(Path(__file__).parent.parent))

import pandas as pd

from app.pull.construction_plan_types_extractor import (
    extract_construction_plan_types,
)
from app.preprocess.construction_plan_types import (
    ConstructionPlanTypesPreprocessor,
)
from app.transform.construction_plan_types_transformer import (
    ConstructionPlanTypesTransformer,
)
from app.load.construction_plan_types_loader import (
    ConstructionPlanTypesLoader,
)


def main(chunksize: int | None = None) -> None:

    output_file = Path("data/staging/construction_plan_types.parquet")

    preprocess = ConstructionPlanTypesPreprocessor()
    transformer = ConstructionPlanTypesTransformer()
    loader = ConstructionPlanTypesLoader()

    writer = None
    transformed_df = None

    data = extract_construction_plan_types(chunksize=chunksize)

    try:
        if isinstance(data, pd.DataFrame):

            clean, writer = preprocess.process(
                data,
                output_file,
                writer,
            )

            transformed_df = transformer.transform(clean)

        else:

            for i, chunk in enumerate(data, start=1):

                print(f"Processing chunk {i}...")

                clean, writer = preprocess.process(
                    chunk,
                    output_file,
                    writer,
                )

                transformed_df = transformer.transform(clean)

                print(clean.head())

    finally:
        if writer is not None:
            writer.close()

    if transformed_df is not None:
        inserted = loader.process(
            transformed_df,
            output_file,
        )

        print(f"{inserted} rows inserted.")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="ETL Job for Construction Plan Types"
    )

    parser.add_argument(
        "--chunksize",
        type=int,
        default=None,
        help=(
            "Number of rows to process per chunk. "
            "If omitted, the entire file is processed at once."
        ),
    )

    args = parser.parse_args()

    main(chunksize=args.chunksize)