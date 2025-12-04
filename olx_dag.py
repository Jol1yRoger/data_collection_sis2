from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from olx_etl_pipeline import scrape_olx_data, process_and_save_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'olx_simple_dag',
    default_args=default_args,
    description='Парсинг OLX',
    schedule='0 10 * * *', 
    catchup=False,
) as dag:

    task_scrape = PythonOperator(
        task_id='scrape_data',
        python_callable=scrape_olx_data,
    )

    task_process = PythonOperator(
        task_id='process_data',
        python_callable=process_and_save_data,
    )

    task_scrape >> task_process