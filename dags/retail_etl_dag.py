from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="retail_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,      # modern syntax instead of schedule_interval
    catchup=False,
    default_args=default_args,
    tags=["retail", "etl", "milestone2"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
    )

    extract_task >> transform_task >> load_task