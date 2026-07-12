
import os
from pathlib import Path
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime


from app.pull.base_extractor import BaseExtractor


class ConstructionPlanTypesExtractor(BaseExtractor):
        
    file_name = "construction_plan_types.csv"
    
    def copy_to_raw(self):
        return super().copy_to_raw(self.source_dir / self.file_name,self.raw_dir)
    
    def extract(self, chunksize=None):
        return super().extract(chunksize=chunksize)
    
def extract_construction_plan_types(
    chunksize=None,
):
    extractor = ConstructionPlanTypesExtractor()

    extractor.copy_to_raw()

    return extractor.extract(chunksize=chunksize)
        
# if __name__ == "__main__":
#     extractor = ConstructionPlanTypesExtractor()

#     result = extractor.extract(chunksize=1)

#     print(result)
        
        
        # def copy_to_landing(raw_path: str, landing_path: str):
        #     raw = Path(raw_path)
        #     landing = Path(landing_path)

        #     landing.mkdir(parents=True, exist_ok=True)

        #     destination = landing / raw.name

        #     shutil.copy2(raw, destination)

        #     print(f"Successfully copied to {destination}")

        # copy_to_landing(
        #     "data/raw/customer.csv",
        #     "data/landing"
        # )
        
        # def copy_to_parquet(self, chunksize=None) -> str:
        #     landing_dir = self.raw_dir
        #     landing_dir.mkdir(parents=True, exist_ok=True)

        #     stem = Path(self.file_name).stem
        #     timestamp = datetime.now().strftime("%Y%m%d")
        #     output_path = landing_dir / f"{stem}_{timestamp}.parquet"

        #     data = self.extract(chunksize=chunksize)

        #     # Small file
        #     if isinstance(data, pd.DataFrame):
        #         data.to_parquet(
        #             output_path,
        #             engine="pyarrow",
        #             compression="snappy",
        #             index=False,
        #         )
        #     # Large file (iterator of DataFrames)
        #     else:
        #         writer = None

        #         for chunk in data:
        #             table = pa.Table.from_pandas(chunk)

        #             if writer is None:
        #                 writer = pq.ParquetWriter(output_path, table.schema)

        #             writer.write_table(table)

        #         if writer:
        #             writer.close()

        #     return str(output_path)

        #     # return str(output_path.resolve())
        
        # def execute(self, chunksize=None) -> str:
        #     return self.copy_to_parquet(chunksize=chunksize)
        
# if __name__ == "__main__":
#     extractor = ConstructionPlanTypesExtractor()

#     result = extractor.execute(chunksize=1)

#     print(result)

# if __name__ == "__main__":
#     extractor = ConstructionPlanTypesExtractor()

#     # result = extractor.extract()

#     result = extractor.copy_to_raw()

#     print(result)
        