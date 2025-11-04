from airflow import DAG
from airflow.operators.python import PythonOperator
from send_email_custom import failure_alert 
from datetime import datetime
import sys
import os

import pendulum

local_tz = pendulum.timezone("Europe/Istanbul")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../projects/ocean/report_base/all_members')))

from ocean_provizyon_gozlem_raporu.main import main as run_etl_main

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1, 0, 0, tzinfo=local_tz),
    'email_on_failure': False,  
    'on_failure_callback': failure_alert, 
    'retries': 0,
}


with DAG(
    dag_id='ocean_provizyon_gozlem_dag',
    default_args=default_args,
    schedule_interval='30 1 * * *',
    catchup=False,
    tags=['ocean', 'etl'],
) as dag:

    etl_task = PythonOperator(
        task_id='run_ocean_provizyon_etl',
        python_callable=run_etl_main,
    )
