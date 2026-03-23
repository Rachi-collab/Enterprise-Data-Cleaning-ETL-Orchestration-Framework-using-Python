from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from src.extract import extract_data
from src.transform import transform_data
from src.load import load_data

default_args = {
    "owner": "airflow",
    "email": ["shaiksadaf1789@gmail.com"],
    "email_on_failure": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="retail_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["etl"],
    default_args=default_args,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
        execution_timeout=timedelta(minutes=15),
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        execution_timeout=timedelta(minutes=15),
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
        execution_timeout=timedelta(minutes=15),
    )

    extract_task >> transform_task >> load_task