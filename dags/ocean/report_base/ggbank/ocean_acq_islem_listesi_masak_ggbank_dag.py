from airflow import DAG
from airflow.operators.python import PythonOperator
from send_email_custom import failure_alert 
from datetime import datetime
import sys
import os

import pendulum

local_tz = pendulum.timezone("Europe/Istanbul")

# Proje dizinini import path'e ekle
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../projects/ocean/report_base/ggbank')))

# Detoken ETL scriptini import et
from ocean_acq_islem_listesi_masak_ggbank.main import main as run_etl_main

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1, 0, 0, tzinfo=local_tz),
    'email_on_failure': False,  
    'on_failure_callback': failure_alert, 
    'retries': 0,
}


with DAG(
    dag_id='ocean_acq_islem_listesi_masak_ggbank_dag',
    default_args=default_args,
    schedule_interval="1 17 * * *", 
    catchup=False,
    tags=['ocean', 'detoken', 'etl', 'ggbank', 'caglar'],
) as dag:

    etl_task = PythonOperator(
        task_id='run_acq_islem_listesi_masak_ggbank_etl',
        python_callable=run_etl_main,
    )
