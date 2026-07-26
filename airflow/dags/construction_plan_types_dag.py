"""
Airflow DAG to trigger the Construction Plan Types ETL job.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='construction_plan_types_etl',
    default_args=default_args,
    description='ETL job for construction plan types data',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'construction_plan_types'],
) as dag:

    run_etl = BashOperator(
        task_id="run_construction_plan_types_etl",
        bash_command="""
        cd /opt/airflow &&
        python -m app.jobs.construction_plan_types_job
        """,
    )