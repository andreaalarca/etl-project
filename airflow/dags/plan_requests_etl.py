"""
ETL DAG for Plan Requests
Independent ETL pipeline for plan requests data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG - CAN RUN ON ANY SCHEDULE (independent)
with DAG(
    dag_id='plan_requests_etl',
    default_args=default_args,
    description='ETL job for plan requests data',
    schedule='@hourly',  # Example: runs every hour (can be set independently)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'plan_requests'],
) as dag:

    # ======================
    # TASK GROUP FACTORY (reused logic)
    # ======================
    def create_table_tasks(table_name: str):
        """Factory function to create ETL task group for a table"""
        with TaskGroup(group_id=table_name, tooltip=f'ETL for {table_name}') as tg:
            # Extract task
            extract = PythonOperator(
                task_id=f'extract_{table_name}',
                python_callable=lambda tn=table_name: __import__(
                    f'app.pull.{tn}_extractor', fromlist=['execute']
                ).execute,
            )

            # Preprocess task
            preprocess = PythonOperator(
                task_id=f'preprocess_{table_name}',
                python_callable=lambda tn=table_name: __import__(
                    f'app.preprocess.{tn}', fromlist=['execute']
                ).execute,
            )

            # Transform task
            transform = PythonOperator(
                task_id=f'transform_{table_name}',
                python_callable=lambda tn=table_name: __import__(
                    f'app.transform.{tn}_transformer', fromlist=['execute']
                ).execute,
            )

            # Load task
            load = PythonOperator(
                task_id=f'load_{table_name}',
                python_callable=lambda tn=table_name: __import__(
                    f'app.load.{tn}_loader', fromlist=['execute']
                ).execute,
            )

            # Internal dependencies: extract → preprocess → transform → load
            extract >> preprocess >> transform >> load

        # Return the TaskGroup for full visibility
        return tg

    # Create and expose the task group
    processing_plan_requests = create_table_tasks('plan_requests')