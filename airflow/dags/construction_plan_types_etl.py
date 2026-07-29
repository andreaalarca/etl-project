"""
ETL DAG for Construction Plan Types
Independent ETL pipeline for construction plan types reference data.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Define the DAG
with DAG(
    dag_id='construction_plan_types_etl',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',  # Example schedule - can be adjusted independently
    catchup=False,
    tags=['etl', 'construction_plan_types'],
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
                ).execute(),
            )

            # Preprocess task
            preprocess = PythonOperator(
                task_id=f'preprocess_{table_name}',
                python_callable=lambda tn=table_name: __import__(
                    f'app.preprocess.{tn}', fromlist=['execute']
                ).execute(),
            )

            # Transform task
            transform = PythonOperator(
                task_id=f'transform_{table_name}',
                python_callable=lambda tn=table_name: __import__(
                    f'app.transform.{tn}_transformer', fromlist=['execute']
                ).execute(),
            )

            # Load task
            load = PythonOperator(
                task_id=f'load_{table_name}',
                python_callable=lambda tn=table_name: __import__(
                    f'app.load.{tn}_loader', fromlist=['execute']
                ).execute(),
            )

            # Internal dependencies: extract → preprocess → transform → load
            extract >> preprocess >> transform >> load

        # Return the TaskGroup for full visibility
        return tg

    # Create and expose the task group
    processing_construction_plan_types = create_table_tasks('construction_plan_types')