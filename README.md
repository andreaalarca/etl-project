# ETL Project: Data Mart Pipeline

This repository contains an end‑to‑end ETL pipeline that extracts data from CSV sources, transforms and loads it into a PostgreSQL data warehouse, and orchestrates the workflow with Apache Airflow.

## Table of Contents
- [Overview](#overview)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Airflow DAGs](#airflow-dags)
- [Extending the Pipeline](#extending-the-pipeline)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Overview

The pipeline consists of four logical stages for each data entity (e.g., `plan_requests`, `customers`, `construction_plan_types`, `dm_plan_requests`):

1. **Extract** – Read raw CSV files from `data/source/` (or copy them to `data/raw/`) and optionally chunk the data for low‑memory processing.
2. **Preprocess** – Clean, validate, and handle missing values according to domain‑specific rules.
3. **Model / Transform** – Perform any required joins, calculations, or type casting. For `dm_plan_requests` this step joins data from the `data_lake` schema (`plan_requests`, `customers`, `construction_plan_types`) and filters by an execution date (YYYY‑MM‑DD).
4. **Load** – Write the processed data to the target warehouse schema (`data_mart`) using high‑performance bulk inserts, then archive the staging file.

All stages are implemented as reusable Python classes that follow a common interface (`extract()`, `execute()`, `load()`). Apache Airflow DAGs orchestrate the execution order, handle retries, and provide monitoring.

---

## Repository Structure

```
.
├── airflow/                     # Airflow DAGs and related config
│   ├── dags/                    # DAG definition files
│   └── logs/                    # Airflow task logs (generated at runtime)
├── app/                         # Core ETL library
│   ├── __init__.py
│   ├── config/                  # Configuration helpers (e.g., warehouse connection)
│   ├── load/                    # Loader implementations
│   ├── model/                   # Model / transform implementations
│   ├── preprocess/              # Preprocessing implementations
│   ├── pull/                    # Extractor implementations
│   ├── transform/               # (Legacy) standalone transformers – now mostly moved into model/
│   └── utils/                   # Logging, helpers
├── data/                        # Data directories (not tracked in Git)
│   ├── raw/                     # Raw CSV files (copied from source)
│   ├── source/                  # Source CSV files (checked into repo for reference)
│   ├── staging/                 # Parquet files produced by ETL steps
│   └── error/                   # Error records (invalid rows)
├── logs/                        # Application‑level logs (optional)
├── .env                         # Environment variables (not committed)
├── .env.example                 # Example environment file
├── requirements.txt             # Python dependencies
├── FLOW.md                      # Detailed description of the DM Plan Requests ETL flow
└── README.md                    # This file
```

> **Note**: The `data/` folder is ignored by Git via `.gitignore`. Place your source CSV files in `data/source/` (they are already present for the sample entities).

---

## Prerequisites

- **Python 3.9+** (the project uses `zoneinfo`; for older Python versions install `backports.zoneinfo` via `pip install backports.zoneinfo`).
- **PostgreSQL 12+** with a database and user configured for the warehouse.
- **Apache Airflow 2.+** (optional, for orchestration). The DAGs can also be run manually via the provided entry points.
- **Git** (to clone the repository).

---

## Installation

1. **Clone the repository**

   ```bash
   git clone <repository-url>
   cd etl-project
   ```

2. **Create a virtual environment (recommended)**

   ```bash
   python -m venv venv
   source venv/bin/activate   # Windows: venv\Scripts\activate
   ```

3. **Install dependencies**

   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Set up environment variables**

   Copy the example file and edit it with your own values:

   ```bash
   cp .env.example .env
   # Edit .env (see the Configuration section below)
   ```

5. **Initialize the warehouse schema**

   The loader classes automatically create the required schema and tables if they do not exist. You can also test the connection manually:

   ```bash
   python -c "from app.config.warehouse_config import WarehouseConfig; w = WarehouseConfig(); print(w.warehouse_url)"
   ```

   Ensure the database user has `CREATEROLE, CREATEDB` privileges or that the schema/tables are pre‑created.

---

## Configuration

All configuration is done via environment variables in the `.env` file (or system environment). The following variables are used:

| Variable | Description | Default |
|----------|-------------|---------|
| `WAREHOUSE_USER` | PostgreSQL username for the warehouse | `airflow` |
| `WAREHOUSE_PASSWORD` | PostgreSQL password | `airflow` |
| `WAREHOUSE_HOST` | PostgreSQL host | `localhost` |
| `WAREHOUSE_PORT` | PostgreSQL port | `5432` |
| `WAREHOUSE_DB` | PostgreSQL database name | `airflow` |
| `WAREHOUSE_SCHEMA_DATA_MART` | Target schema for data marts | `data_mart` |
| `WAREHOUSE_SCHEMA_DATA_LAKE` | Source schema for raw data | `data_lake` |
| `RAW_DIR` | Directory where extracted CSV files are stored (after copy from source) | `./data/raw` |
| `STAGING_DIR` | Directory for Parquet staging files | `./data/staging` |
| `ERROR_DIR` | Directory for invalid records | `./data/error` |
| `ARCHIVE_DIR` | Base directory for archived staging files | `./data/archive` |
| `BATCH_SIZE` | Number of rows per batch when loading into PostgreSQL | `1000` |
| `CHUNK_SIZE` | Number of rows per chunk when reading CSVs (set to `0` or empty to load whole file) | `None` |
| `LOG_LEVEL` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`) | `INFO` |

**Example `.env`**

```dotenv
WAREHOUSE_USER=my_user
WAREHOUSE_PASSWORD=secret
WAREHOUSE_HOST=db.mycompany.com
WAREHOUSE_PORT=5432
WAREHOUSE_DB=dw
WAREHOUSE_SCHEMA_DATA_MART=data_mart
WAREHOUSE_SCHEMA_DATA_LAKE=data_lake
RAW_DIR=data/raw
STAGING_DIR=data/staging
ERROR_DIR=data/error
ARCHIVE_DIR=data/archive
BATCH_SIZE=5000
CHUNK_SIZE=10000
LOG_LEVEL=INFO
```

---

## Running the Pipeline

### Option 1: Run a Single ETL Job Manually

Each entity provides a convenience `execute()` function that runs the full extract → preprocess → model → load → archive sequence.

```bash
# Example: run the plan_requests ETL for a specific date (YYYY-MM-DD)
python -c "from app.model.plan_requests_model import execute; print(execute('2024-06-01'))"

# Example: run with default date (today in Philippines timezone)
python -c "from app.model.plan_requests_model import execute; print(execute())"
```

The same pattern exists for `customers`, `construction_plan_types`, and `dm_plan_requests`.

### Option 2: Run via Airflow

1. **Start Airflow** (if not already running)

   ```bash
   # Initialize the metadata DB (first time only)
   airflow db init

   # Create an admin user (first time only)
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com

   # Start the webserver and scheduler in separate terminals
   airflow webserver --port 8080
   airflow scheduler
   ```

2. **Access the UI** at <http://localhost:8080> (log in with the admin user you created).

3. **Trigger a DAG**  
   - Find the DAG you want to run (e.g., `dm_plan_requests_etl`).  
   - Click the **Trigger DAG** button (or set a schedule in the DAG file).  
   - Monitor progress in the **Tree View** or **Graph View**.

4. **View Logs**  
   - Click on a task instance → **Log** to see the execution output.  
   - Application logs are also written to `logs/etl_pipeline.log` (configured via logging setup).

---

## Airflow DAGs

| DAG File | Description | Schedule |
|----------|-------------|----------|
| `airflow/dags/dm_plan_requests_etl.py` | Joins `plan_requests`, `customers`, and `construction_plan_types` into the `data_mart.dm_plan_requests` table. | `None` (manual trigger) – adjust `schedule` in the file if you want a periodic run. |
| `airflow/dags/plan_requests_etl.py` | Independent ETL for the `plan_requests` entity (extract → preprocess → model → load). | `@hourly` (example) |
| `airflow/dags/customers_etl.py` | Independent ETL for the `customers` entity. | `@daily` (example) |
| `airflow/dags/construction_plan_types_etl.py` | Independent ETL for the `reference` table `construction_plan_types`. | `@weekly` (example) |
| `airflow/dags/multi_table_etl_pipeline.py` | Example pipeline that triggers multiple downstream DAGs. | `@daily` (example) |

All DAGs use a `TaskGroup` to keep the UI tidy and rely on the same underlying `app` modules for the actual work.

---

## Extending the Pipeline

To add a new entity (e.g., `orders`):

1. **Create the source CSV**  
   Place it in `data/source/orders.csv` (or add to the existing source folder).

2. **Implement the four components** under `app/`:

   - **Extractor** – `app/pull/orders_extractor.py` (subclass `BaseExtractor` or implement `extract()`).
   - **Preprocessor** – `app.preprocess.orders` (a module with a `Processor` class and an `execute()` function).
   - **Model / Transform** – `app.model.orders_model` (if you need joins or calculations; otherwise a simple pass‑through).
   - **Loader** – `app.load.orders_loader` (subclass `BaseLoader` or implement `execute()` that reads the staged Parquet and loads to the warehouse).

3. **Add an Airflow DAG**  
   Create a new file in `airflow/dags/` (e.g., `orders_etl.py`) that follows the pattern of the existing DAGs: create a `TaskGroup` with four `PythonOperator` tasks (extract, preprocess, model, load) and set the dependencies.

4. **Update `.gitignore` if needed**  
   Ensure any new directories under `data/` (e.g., `data/source/orders`) are already covered by the generic `data/` rule.

5. **Test locally**  
   Run the `execute()` function from the model or loader to verify the end‑to‑end flow before committing.

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| **ModuleNotFoundError: No module named 'zoneinfo'** | Running on Python <3.9 without `backports.zoneinfo` installed. | `pip install backports.zoneinfo` |
| **Connection refused to PostgreSQL** | Wrong `WAREHOUSE_*` credentials or DB not reachable. | Validate `.env` values; test with `psql -h $WAREHOUSE_HOST -U $WAREHOUSE_USER -d $WAREHOUSE_DB`. |
| **FileNotFoundError for staging file** | The upstream task (model/transform) did not produce a file, or the `execution_date` mismatch. | Examine the logs of the preceding task; ensure the date passed matches the date used in the file name. |
| **Loader falls back to SQLAlchemy and is slow** | `psycopg2-binary` missing or incompatible version. | `pip install psycopg2-binary` |
| **DAG shows “task failed” but logs look fine** | An exception was caught and logged but not re‑raised, so Airflow marks the task as success. | Ensure all exceptions are re‑raised (`raise`) after logging so Airflow treats the task as failed. |
| **Duplicate key errors on reload** | The loader tries to insert existing rows (no upsert logic). | For idempotent loads, either delete existing rows for the date before loading, or implement an upsert (`ON CONFLICT … DO UPDATE`). Currently the loader drops/recreates the table in `_ensure_schema_and_table` – this is intended for dev; adjust if you need to retain historical data. |

---

## License

This project is licensed under the MIT License – see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- The project structure draws on common data‑engineering best practices.
- **Apache Airflow** provides reliable orchestration and monitoring.
- **PostgreSQL** offers a robust, open‑source warehouse.
- **pandas** and **SQLAlchemy** simplify data manipulation and database interaction.

---

**Happy data engineering!** If you have questions or suggestions, please open an issue or submit a pull request.