# ETL Flow Explanation (DM Plan Requests Job)

This document explains the end‑to‑end flow of the **dm_plan_requests_etl** DAG, focusing on how the modular components (extractor, preprocessor, model‑transformer, loader) interact, how staging files are handled, and the archiving process.

--- 

## 1. High‑level Pipeline

```
Extractor  →  Preprocessor  →  Model (Transformer)  →  Loader
   │               │                │                   │
   ▼               ▼                ▼                   ▼
Source file   Cleaned DF     Joined & Typed DF   Loaded into Warehouse
                                                                    │
                                                                    ▼
                                                         Archive (staging → archive)
```

The DAG orchestrates four main tasks within a `TaskGroup`:

1. **Extractor** – pulls raw data from source CSV files (via `app.pull.<table>_extractor`).
2. **Preprocessor** – cleans and validates the raw data (via `app.preprocess.<table>`).
3. **Model** – performs the data‑model transformation (joins, type casting) and writes the result to a staging Parquet file (via `app.model.<table>_model`).
4. **Loader** – reads the staged Parquet file and bulk‑loads it into the target warehouse table (via `app.load.<table>_loader`).

After the loader succeeds, the staging file is moved to an archive directory.

--- 

## 2. Extractor Output

The extractor follows the same pattern used across the project:

| `chunksize` argument | Return type of `extract_<table>(chunksize=…)` |
|----------------------|-----------------------------------------------|
| **`None`** (default) | A single `pandas.DataFrame` containing the whole file. |
| **Integer > 0**      | A **generator** yielding `pandas.DataFrame` objects, each ≤ `chunksize` rows. |

*The extractor only reads the source file; it never writes to disk.*

--- 

## 3. Preprocessor (Staging File Handling)

The preprocessor receives an optional `writer` argument (a `pyarrow.ParquetWriter`):

* **First call**: creates a Parquet writer pointing at the staging file path  
  (`<staging_dir>/<table>_<execution_date>.parquet`).
* **Subsequent calls**: receives the same writer, appends the cleaned chunk, and returns the updated writer.
* After processing all chunks, the DAG’s `finally` block calls `writer.close()` to finalize the file.

**Result:** Whether the data arrived as one DataFrame or many chunks, **exactly one** Parquet file ends up in the staging directory for the given execution date.

--- 

## 4. Model (Transformer) Responsibilities

The model class (`DmPlanRequestsModel`) now **embeds the transformation logic** that previously lived in a separate `datamart.transform` module. Its `execute(execution_date)` method:

1. Receives an `execution_date` string (`YYYY-MM-DD`) – either from the Airflow context (`ds`) or, when `None`, defaults to **today’s date in the Philippines timezone (Asia/Manila)**.
2. Instantiates the internal transformer (`DMPLANREQUESTSTransformer`) which:
   * Builds a SQL query that joins `data_lake.plan_requests`, `data_lake.customers`, and `data_lake.construction_plan_types`.
   * **Always** applies a `WHERE pr.request_date = DATE 'YYYY-MM-DD'` filter using the (possibly defaulted) execution date.
   * Executes the query via SQLAlchemy, returning a joined `DataFrame`.
   * Performs final data‑type conversions to match the target warehouse schema.
3. Writes the resulting DataFrame to the staging Parquet file (path determined by `_get_staging_file_path(execution_date)`).
4. Returns a status string indicating success and the number of rows processed.

Because the filtering happens **inside the SQL query**, only the relevant day’s data is ever read from the source tables, making the step efficient.

--- 

## 5. Loader Responsibilities

The loader (`DmPlanRequestsLoader`) receives the execution date to locate the correct staging file:

* Its `execute(execution_date)` method:
  1. Builds the expected staging file path: `<staging_dir>/dm_plan_requests_<execution_date>.parquet`.
  2. Reads the entire Parquet file into a `DataFrame`.
  3. Performs a high‑performance bulk insert into the warehouse table `data_mart.dm_plan_requests` using:
     - Primary path: `psycopg2.extras.execute_values`.
     - Fallback: SQLAlchemy `to_sql` with `method="multi"`.
  4. After a successful load, calls `archive()` to move the staging file to a timestamped subdirectory under the archive root.
  5. Returns a status string with the number of rows loaded.

The loader never reads or modifies the source data directly; it works solely with the staged Parquet file produced by the model.

--- 

## 6. Archiving Step

After the loader task completes successfully, the DAG executes an archive step (handled inside the loader’s `execute` method, but conceptually a separate phase):

* `loader.archive(source_file: Path)` moves the file from  
  `<staging_dir>/dm_plan_requests_<execution_date>.parquet`  
  to  
  `<archive_root>/<timestamp>/dm_plan_requests_<execution_date>.parquet`  
  where `<timestamp>` is `YYYYMMDD_HHMMSS` of the archival moment.

* Because there is **only one** staging file per execution date, the archive step moves exactly one file.
* If any step fails before the loader completes, the staging file is **not** archived; instead, it remains in the staging directory for manual inspection (or can be cleared by a cleanup process).

--- 

## 7. Diagram of the Flow (with optional chunking)

```
Source file (CSV)
        │
Extractor (generator of chunks) ──► ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
        ▼                           ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼
Preprocessor (receives writer)  Chunk‑1 Chunk‑2 … Chunk‑k
        │                           │   │   │   │   │   │   │   │   │
        ▼                           ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼
Model (Transformer + Writer)  ↓   ↓   ↓   ↓   ↓   ↓   ↓   ↓   ↓   ▼
        │                    (Cleaned & validated chunks)
        ▼                           ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼   ▼
                                 Join + Type Casting (SQL) 
        │                                   │
        ▼                                   ▼
                     Joined & Typed DF (single)
        │                                   │
        ▼                                   ▼
                     Write Staging Parquet (one file)
        │                                   │
        ▼                                   ▼
                     Loader (bulk insert) 
        │                                   │
        ▼                                   ▼
                 Inserted into Warehouse
        │                                   │
        ▼                                   ▼
                 Archive Step 
        │                                   │
        ▼                                   ▼
   Move staging file to archive/<timestamp>/
```

*Note:* Even if the extractor yields multiple chunks, the model writes **only one** Parquet file per execution date because the writer is created once and reused across all chunks (the same pattern used by other ETL jobs in this repo).

--- 

## 8. Key Take‑aways

* **Execution date handling:**  
  - When the DAG runs, Airflow supplies `ds` (YYYY-MM-DD) → used as the `execution_date`.  
  - If the model is invoked manually without an argument, it defaults to **today’s date in the Philippines timezone (Asia/Manila)**, formatted as YYYY-MM-DD.  
  - The model’s transformer **always** applies a `WHERE pr.request_date = DATE 'YYYY-MM-DD'` clause, ensuring only the relevant day’s data is processed.

* **Single staging file per run:**  
  - Regardless of chunking, exactly one Parquet file (`dm_plan_requests_<YYYY-MM-DD>.parquet`) is created in the staging directory.  
  - Consequently, the archive step moves a single file, simplifying traceability and cleanup.

* **Efficiency:**  
  - Filtering occurs at the database level (inside the SQL join), minimizing data transfer.  
  - The loader uses `psycopg2.extras.execute_values` for high‑performance bulk inserts, with a safe fallback to SQLAlchemy.

* **Modularity:**  
  - The transformation logic is now encapsulated within the model class (`app.model.dm_plan_requests_model`), eliminating the need for a separate `datamart.transform` module.  
  - The loader is a standalone implementation in `app.load.dm_plan_requests_loader` (the previous delegating wrapper has been replaced with the full class).

* **Fault tolerance:**  
  - If any task fails, the workflow stops and the staging file remains in place for inspection (not archived).  
  - Successful runs automatically archive the staging file, preserving a copy for auditing or reprocessing if needed.

--- 

*This document reflects the current state of the `dm_plan_requests_etl` DAG after the recent refactor to consolidate transformation logic into the model layer and to unify the loader implementation, with execution date expected in YYYY-MM-DD format.*