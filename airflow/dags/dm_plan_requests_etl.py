"""
ETL DAG for DM Plan Requests
Data mart ETL pipeline that joins multiple source tables from data_lake schema.
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
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
}

# Define the DAG - manually triggered (schedule=None)
with DAG(
    dag_id='dm_plan_requests_etl',
    default_args=default_args,
    description='ETL job for data mart plan requests (joined data from data_lake)',
    schedule=None,  # Set to None for manual triggering; adjust as needed
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'datamart', 'dm_plan_requests'],
) as dag:

    # TaskGroup factory (reused logic for dm_plan_requests)
    def create_dm_plan_requests_tasks():
        """Factory function to create ETL task group for dm_plan_requests"""
        with TaskGroup(group_id='dm_plan_requests', tooltip='ETL for dm_plan_requests') as tg:
            # Model task
            def model_task(**context):
                """Execute the model transformation and return DataFrame via XCom"""
                # Import here to avoid circular imports
                from app.model.dm_plan_requests_model import execute

                # Get execution date from context - using ds (YYYY-MM-DD format) for date filtering
                execution_date = context['ds']  # YYYY-MM-DD format

                # Execute the model - returns status message
                result = execute(execution_date=execution_date)

                return f"SUCCESS: Modeled dm_plan_requests records for {execution_date}"

            model_op = PythonOperator(
                task_id='model',
                python_callable=model_task,
            )

            # Load task
            def load_task(**context):
                """Load data from staging area into the data_mart schema"""
                # Import here to avoid circular imports
                from app.load.dm_plan_requests_loader import execute

                # Get execution date from context - using ds (YYYY-MM-DD format) for file path
                execution_date = context['ds']  # YYYY-MM-DD format

                # Execute the load - returns status message
                result = execute(execution_date=execution_date)

                return f"SUCCESS: Loaded dm_plan_requests records into warehouse for {execution_date}"

            load_op = PythonOperator(
                task_id='load',
                python_callable=load_task,
            )

            # Set dependencies: model -> load
            model_op >> load_op

        # Return the TaskGroup for full visibility
        return tg

    # Create and expose the task group
    dm_plan_requests_etl = create_dm_plan_requests_tasks()

    # No upstream dependencies - this DAG runs when triggered (manually or by other DAGs)