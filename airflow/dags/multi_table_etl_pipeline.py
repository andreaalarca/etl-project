"""
Multi-table ETL Pipeline with stage-level visibility
Implements factory pattern and dataset-based dependencies
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# Try to import Dataset from different possible locations for compatibility
try:
    from airflow.dataset import Dataset
except ImportError:
    try:
        from airflow.datasets import Dataset
    except ImportError:
        try:
            from airflow.models.dataset import Dataset
        except ImportError:
            Dataset = None

# Define dataset variables only if Dataset is available, otherwise set to None
if Dataset is not None:
    DATASET_CONSTRUCTION_PLAN_TYPES = Dataset("warehouse_table://construction_plan_types")
    DATASET_CUSTOMERS = Dataset("warehouse_table://customers")
    DATASET_PLAN_REQUESTS = Dataset("warehouse_table://plan_requests")
    DATASET_DM_PLAN_REQUESTS = Dataset("warehouse_table://dm_plan_requests")
else:
    DATASET_CONSTRUCTION_PLAN_TYPES = None
    DATASET_CUSTOMERS = None
    DATASET_PLAN_REQUESTS = None
    DATASET_DM_PLAN_REQUESTS = None

# ======================
# DAG CONFIGURATION
# ======================
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='multi_table_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for multiple interdependent tables using datasets',
    schedule=None,  # Set to None as it's triggered externally or via dependencies
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['etl', 'multi-table', 'dataset'],
) as dag:

    # ======================
    # TASK GROUP FACTORY
    # ======================
    def create_table_tasks(table_name: str, dataset: Dataset = None, inlet_dataset: Dataset = None):
        """Factory function to create ETL task group for a table"""
        with TaskGroup(group_id=table_name, tooltip=f'ETL for {table_name}') as tg:
            # Extract task
            extract = PythonOperator(
                task_id=f'extract_{table_name}',
                python_callable=lambda tn=table_name: __import__(
                    f'app.pull.{tn}_extractor', fromlist=['execute']
                ).execute(),
                # If this table has an inlet dataset (dependency), wait for it
                inlets=[inlet_dataset] if inlet_dataset else None,
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
                # This task outlets its dataset (signals that the table has been updated)
                outlets=[dataset] if dataset else None,
            )

            # Internal dependencies: extract → preprocess → transform → load
            extract >> preprocess >> transform >> load

        # Return the TaskGroup (not just the load task) for full visibility
        return tg

    # ======================
    # CREATE ALL TABLE TASK GROUPS
    # ======================
    # Base table (no upstream deps)
    processing_construction_plan_types = create_table_tasks(
        'construction_plan_types',
        dataset=DATASET_CONSTRUCTION_PLAN_TYPES
    )

    # Dependent tables (all depend on construction_plan_types being loaded)
    processing_customers = create_table_tasks(
        'customers',
        dataset=DATASET_CUSTOMERS,
        inlet_dataset=DATASET_CONSTRUCTION_PLAN_TYPES
    )

    processing_plan_requests = create_table_tasks(
        'plan_requests',
        dataset=DATASET_PLAN_REQUESTS,
        inlet_dataset=DATASET_CONSTRUCTION_PLAN_TYPES
    )

    processing_dm_plan_requests = create_table_tasks(
        'dm_plan_requests',
        dataset=DATASET_DM_PLAN_REQUESTS,
        inlet_dataset=DATASET_CONSTRUCTION_PLAN_TYPES
    )

    # ======================
    # DEPENDENCY MAP (FAN-OUT PATTERN - DATASET-BASED)
    # ======================
    # With dataset-based dependencies defined in the tasks above (inlets/outlets),
    # we don't need to set explicit task dependencies here.
    # The Airflow scheduler will handle triggering based on dataset updates.
    #
    # However, since this DAG has schedule=None and is meant to be triggered externally,
    # we'll keep the task groups independent but document the dataset relationships:
    #
    # - processing_construction_plan_types: outlets DATASET_CONSTRUCTION_PLAN_TYPES
    # - processing_customers: inlets DATASET_CONSTRUCTION_PLAN_TYPES, outlets DATASET_CUSTOMERS
    # - processing_plan_requests: inlets DATASET_CONSTRUCTION_PLAN_TYPES, outlets DATASET_PLAN_REQUESTS
    # - processing_dm_plan_requests: inlets DATASET_CONSTRUCTION_PLAN_TYPES, outlets DATASET_DM_PLAN_REQUESTS
    #
    # When run as a single DAG, all tasks will execute in sequence based on their
    # internal dependencies (extract -> preprocess -> transform -> load).
    # The dataset annotations are for documentation and potential future use
    # with dataset-triggered DAGs or cross-DAG dependencies.