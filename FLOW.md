# ETL Flow for Construction Plan Types

## Overview
This document describes the ETL process for construction plan types data as implemented in `app/jobs/construction_plan_types_job.py`.

## Flow Breakdown

### Pull (Extract)
- **Module**: `app.pull.construction_plan_types_extractor:ConstructionPlanTypesExtractor`
- **Responsibilities**:
  - Receives the source file path from the environment variable `SOURCE_FILE_PATH`.
  - Copies the source file to the raw data directory (to preserve the original).
  - Reads the source file (supporting CSV, Excel, TXT, TSV) with optional chunking (via `CHUNK_SIZE` environment variable).
  - Returns either a single DataFrame or an iterator of DataFrames (for chunked processing).

### Preprocess
- **Module**: `app.preprocess.construction_plan_types:ConstructionPlanTypesPreprocessor`
- **Responsibilities**:
  - Validates the file structure (checks for empty file and missing columns).
  - Normalizes header names: strips whitespace, converts to lowercase, replaces spaces and hyphens with underscores.
  - Validates that all required source columns are present.
  - Cleans string values (strips whitespace and replaces "nan" with pd.NA).
  - Handles missing values according to a predefined strategy (e.g., drop rows with missing IDs, fill strings with empty string, etc.).
  - Returns a cleaned DataFrame (or combines cleaned chunks if processing in chunks).

### Transform
- **Module**: `app.transform.construction_plan_types_transformer:ConstructionPlanTypesTransformer`
- **Responsibilities**:
  - Maps source column names to SQL column names using a predefined mapping.
  - Validates that required SQL columns are present after mapping.
  - Adds any missing destination columns with default values (or NULL).
  - Reorders columns to match the SQL schema.
  - Converts data types to the appropriate SQL types (e.g., Int64 for IDs, float64 for prices, string for text).
  - Applies business rules (e.g., setting negative prices to zero, removing rows with null IDs).
  - (Optionally) creates derived or calculated columns.
  - Removes any unnecessary columns (keeping only those in the SQL schema).
  - Returns a transformed DataFrame ready for loading.

### Load
- **Module**: `app.load.construction_plan_types_loader:ConstructionPlanTypesLoader`
- **Responsibilities**:
  - Reads the transformed DataFrame (passed directly from the Transform step).
  - Performs batch inserts into the database using the configured batch size.
  - Handles transactions via SQLAlchemy's context manager (automatic commit on success, rollback on failure).
  - Returns the number of inserted rows.
  - (Note: The archiving of the source file is handled in the job after loading, not in the loader.)

### Job Orchestration (in `app/jobs/construction_plan_types_job.py`)
- **Main Function**: `main()`
- **Steps**:
  1. Load environment variables.
  2. Validate and set up configuration (source file path, directories, chunk size).
  3. Ensure required directories exist (raw, staging, error, archive).
  4. Validate the source file exists and is readable.
  5. **Pull**: 
       - Copy the source file to the raw directory.
       - Extract data from the copied file (with optional chunking).
  6. **Preprocess**: 
       - Clean the extracted data (handle both single DataFrame and chunked iterator).
  7. **Transform**: 
       - Transform the preprocessed data to match SQL schema and apply business rules.
  8. **Load**: 
       - Load the transformed data into the database (using the loader's load method).
  9. **Archive Parquet**: 
       - Save the transformed data as a Parquet file in the staging directory for auditability.
  10. **Archive Source**: 
       - Move the copied source file from the raw directory to the archive directory (with timestamp to avoid overwriting).
  11. Log success or failure and return appropriate exit code.

## Environment Variables
- `SOURCE_FILE_PATH`: (Required) Full path to the source file.
- `CHUNK_SIZE`: (Optional) Number of rows per chunk for chunked processing (applies to CSV, TXT, TSV).
- `RAW_DIR`: (Default: `./data/raw`) Directory for raw data copies.
- `STAGING_DIR`: (Default: `./data/staging`) Directory for staged Parquet files.
- `ERROR_DIR`: (Default: `./data/error`) Directory for error records.
- `ARCHIVE_DIR`: (Default: `./data/archive/construction_plan_types`) Directory for archived source files.
- Plus the variables used by the loader for database connection (WAREHOUSE_URL, SCHEMA, etc.) and batch size (BATCH_SIZE).

## Notes
- The original source file is never modified; only a copy is used for processing.
- The job supports both single-file and chunked processing (for applicable file types).
- Error handling is in place: any exception during the process will be caught, logged, and the job will return a failure status.
- The Parquet file saved in the staging directory is for auditing and can be used for reprocessing if needed.