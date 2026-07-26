"""
ETL DAG for DM Plan Requests
Data mart ETL pipeline that joins multiple source tables from data_lake schema.
Uses Dataset Scheduling to wait for upstream data_lake datasets.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.datasets import Dataset

# Define the datasets that this DAG depends on (assuming these are outlet by the Data Lake DAGs)
# If the Data Lake DAGs use different dataset aliases, update these strings accordingly.
plan_requests_dataset = Dataset("plan_requests")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG - Schedule triggered by updates to all three upstream datasets
with DAG(
    dag_id='dm_plan_requests_etl',
    default_args=default_args,
    description='ETL job for data mart plan requests (joined data from data_lake)',
    schedule=[plan_requests_dataset],  # Wait for all three datasets
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'datamart', 'dm_plan_requests'],
) as dag:

    # ======================
    # MODEL TASK
    # ======================
    # This task runs the model: reads from data_lake, joins, writes to staging
    model_task = PythonOperator(
        task_id='model',
        python_callable=lambda: __import__(
            'app.transform.dm_plan_requests_model', fromlist=['execute']
        ).execute,
    )

    # ======================
    # LOAD TASK
    # ======================
    # This task runs the load: reads from staging, loads into data_mart
    load_task = PythonOperator(
        task_id='load',
        python_callable=lambda: __import__(
            'app.load.dm_plan_requests_loader', fromlist=['execute']
        ).execute,
    )

    # SET DEPENDENCIES: Model must complete before Load
    model_task >> load_task