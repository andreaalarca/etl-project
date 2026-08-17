"""
ETL DAG for DM Plan Requests
"""
from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from app.data_mart.models.dm_plan_requests import execute as model_execute
from app.data_mart.validation.dm_plan_requests import execute as validate_execute
from app.data_mart.materialization.dm_plan_requests import execute as load_execute


with DAG(
    dag_id='dm_plan_requests_etl2',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=['etl2', 'datamart2', 'dm_plan_requests2'],
) as dag:

    model = PythonOperator(
        task_id='model',
        python_callable=model_execute,
        op_kwargs={'process_date': '{{ ds }}'}
    )

    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_execute,
        op_kwargs={
            'staging_file_path': "{{ ti.xcom_pull(task_ids='model')['staging_file_path'] }}",
            'process_date': "{{ ti.xcom_pull(task_ids='model')['process_date'] }}",
        }
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_execute,
        op_kwargs={
            'staging_file_path': "{{ ti.xcom_pull(task_ids='validate')['staging_file_path'] }}",
            'process_date': "{{ ti.xcom_pull(task_ids='validate')['process_date'] }}",
        }
    )

    model >> validate >> load