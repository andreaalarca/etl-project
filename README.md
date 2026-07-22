# ETL Project

A modular ETL pipeline built with **Python**, **Apache Airflow**, **PostgreSQL**, and **Docker** featuring automatic schema/table creation and secure configuration management.

## Tech Stack

- Python 3.13+
- Apache Airflow 3
- PostgreSQL 16
- Redis
- Docker & Docker Compose
- pgAdmin 4

---

# Prerequisites

Install the following software before running the project.

## 1. Git

Download:

https://git-scm.com/downloads

Verify installation:

```bash
git --version
```

---

## 2. Python

Download Python 3.13 or later:

https://www.python.org/downloads/

Verify installation:

```bash
python --version
```

---

## 3. Docker Desktop

Download:

https://www.docker.com/products/docker-desktop/

After installation:

- Enable WSL2
- Start Docker Desktop

Verify:

```bash
docker --version
docker compose version
```

---

## 4. Visual Studio Code

Download:

https://code.visualstudio.com/

Recommended Extensions

- Python
- Pylance
- Docker
- YAML
- GitLens

---

# Clone Repository

```bash
git clone <repository-url>

cd etl-project
```

---

# Create Virtual Environment

Windows

```bash
python -m venv venv
```

Activate

PowerShell

```powershell
.\venv\Scripts\Activate.ps1
```

Command Prompt

```cmd
venv\Scripts\activate
```

---

# Install Python Packages

```bash
pip install -r requirements.txt
```

---

# Configure Environment Variables

Create a `.env` file in the project root. This file contains both ETL application configuration and Airflow service configuration.

Example:

```env
# ============================================================================
# ETL APPLICATION CONFIGURATION
# ============================================================================

# Dataset type (DATA_LAKE or DATA_SET)
DATASET_TYPE=DATA_LAKE

# Database Configuration for ETL Operations
POSTGRES_DATA_LAKE_HOST=localhost
POSTGRES_DATA_LAKE_PORT=5432
POSTGRES_DATA_LAKE_DB=analytics
POSTGRES_DATA_LAKE_USER=postgres
POSTGRES_DATA_LAKE_PASSWORD=your_password

SCHEMA=data_lake

# File Paths for ETL Processing
RAW_DIR=./data/raw
STAGING_DIR=./data/staging
ARCHIVE_DIR=./data/archive
ERROR_DIR=./data/error

# Processing Configuration
BATCH_SIZE=1000
MAX_RETRIES=3
DRY_RUN=false

# ============================================================================
# AIRFLOW SERVICE CONFIGURATION (for Docker Compose)
# ============================================================================

# Airflow User ID (for file permissions in containers)
AIRFLOW_UID=50000

# PostgreSQL Service Credentials (used by Airflow metadata database)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=analytics

# pgAdmin4 Credentials
PGADMIN_DEFAULT_EMAIL=andreaalarca@gmail.com
PGADMIN_DEFAULT_PASSWORD=andrea123

# JWT Authentication for Airflow API
AIRFLOW__API_AUTH__JWT_SECRET=airflow_jwt_secret
AIRFLOW__API_AUTH__JWT_ISSUER=airflow

# Airflow Web Admin Credentials
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# Additional PIP requirements (if any)
_PIP_ADDITIONAL_REQUIREMENTS=

# ============================================================================
# DRY RUN / TEST CONFIGURATION (optional)
# ============================================================================

# Test database configuration (when DRY_RUN=true)
TEST_POSTGRES_DB_SCHEMA_DATA_LAKE_NAME=analytics_test
TEST_POSTGRES_DB_SCHEMA_DATA_SET_NAME=analytics_test
HEMA_DATA_SET_PASSWORD=andrea123
TA_SET_PASSWORD=andrea123
ATA_SET_PASSWORD=andrea123
```

> **Note**
>
> - These settings are used when running the ETL directly from your local machine
> - The Airflow service configuration variables are used by Docker Compose to configure the Airflow stack
> - The `.env.example` file in the project root shows the expected structure without actual values

---

# Running the ETL (Without Airflow)

Activate your virtual environment:

```powershell
.\venv\Scripts\Activate.ps1
```

Run the ETL job:

```bash
python -m app.jobs.construction_plan_types_job
```

---

# Airflow Setup

Navigate to the Airflow folder.

```bash
cd airflow
```

> **Note**
>
> Airflow service configuration (database credentials, pgAdmin settings, etc.) is now managed through the `.env` file in the project root. No separate `.env` file is needed in the airflow/ directory.

Verify your project root `.env` contains the required Airflow service variables (see "Configure Environment Variables" section above).

---

# Start Airflow

```bash
docker compose up -d
```

Verify all services are running.

```bash
docker compose ps
```

Expected services:

- airflow-apiserver
- airflow-scheduler
- airflow-worker
- airflow-triggerer
- airflow-dag-processor
- postgres
- redis
- pgadmin

---

# Stop Airflow

```bash
docker compose down
```

---

# Restart Airflow

```bash
docker compose up -d
```

---

# Airflow Web UI

Open:

```
http://localhost:8080
```

Default credentials

```
Username: airflow
Password: airflow
```

---

# PostgreSQL

Docker PostgreSQL configuration

| Property | Value |
|----------|-------|
| Host | postgres |
| Port | 5432 |
| Database | analytics |
| Username | airflow |
| Password | airflow |

---

# pgAdmin

Open:

```
http://localhost:5050
```

Login

```
Email:
andreaalarca@gmail.com

Password:
andrea123
```

---

# Add PostgreSQL Server

### General

```
Name:
Docker PostgreSQL
```

### Connection

| Property | Value |
|----------|-------|
| Host | postgres |
| Port | 5432 |
| Maintenance Database | analytics |
| Username | airflow |
| Password | airflow |

Click **Save**.

---

# Running the DAG

Open Airflow.

Select:

```
construction_plan_types_etl
```

Click:

```
Trigger DAG
```

---

# Verify Inserted Data

Open pgAdmin.

Navigate to

```
Servers
└── Docker PostgreSQL
    └── Databases
        └── analytics
            └── Schemas
                └── data_lake
                    └── Tables
                        └── construction_plan_types
```

Execute:

```sql
SELECT *
FROM data_lake.construction_plan_types;
```

---

# Reset Test Data

```sql
TRUNCATE TABLE data_lake.construction_plan_types;
```

---

# Useful Docker Commands

Start

```bash
docker compose up -d
```

Stop

```bash
docker compose down
```

Restart

```bash
docker compose restart
```

View logs

```bash
docker compose logs -f
```

List containers

```bash
docker compose ps
```

Open PostgreSQL shell

```bash
docker compose exec postgres psql -U airflow -d analytics
```

Exit PostgreSQL

```sql
\q
```

---

# Project Structure

```text
etl-project/
│
├── app/
│   ├── config/
│   ├── jobs/
│   ├── load/
│   ├── preprocess/
│   ├── pull/
│   ├── transform/
│   └── utils/
│
├── airflow/
│   ├── config/
│   ├── dags/
│   ├── logs/
│   ├── plugins/
│   ├── docker-compose.yaml
│   └── .env
│
├── data/
│   ├── archive/
│   ├── error/
│   ├── raw/
│   └── staging/
│
├── requirements.txt
└── README.md
```

---

# ETL Flow

```
CSV
    │
    ▼
Extract
    │
    ▼
Preprocess
    │
    ▼
Transform
    │
    ▼
Load
    │
    ▼
PostgreSQL
    │
    ▼
Airflow DAG
```

---

# Author

Developed as a modular ETL pipeline using Python and Apache Airflow for learning, portfolio, and production-ready ETL workflows.