from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/scripts")

from scripts.upload_to_gcs import upload_to_gcs
from scripts.transform_data import transform_data
from scripts.load_to_bigquery import load_to_bigquery

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="retail_sales_etl_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False,
    default_args=default_args
) as dag:

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_file
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_to_bigquery",
        python_callable=load_to_bigquery
    )

    upload_task >> transform_task >> load_task