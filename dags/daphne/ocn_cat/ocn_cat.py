import os, sys
from send_email_custom import failure_alert 
# DAG dosyanızın bulunduğu klasörden projenin src dizinine kadar giden yol
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../../projects/daphne/ocn_cat')
)
SRC_DIR = os.path.join(BASE_DIR, 'src')

# Öncelikle burayı import yolunun başına koyuyoruz
sys.path.insert(0, SRC_DIR)





import yaml
import json
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from etl_manager import ETLManager

import pendulum

local_tz = pendulum.timezone("Europe/Istanbul")


def run_etl_task(task_conf, source_db_conf, target_db_conf):
    task_conf['source_db_config'] = source_db_conf
    task_conf['target_db_config'] = target_db_conf

    etl_manager = ETLManager(source_db_conf, target_db_conf)
    etl_manager.start_etl(**task_conf)

def get_db_config_from_airflow(variable_name: str):
    try:
        return json.loads(Variable.get(variable_name))
    except KeyError:
        print(f"[WARN] Airflow Variable '{variable_name}' bulunamadı.")
        return None

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1, 0, 0, tzinfo=local_tz),
    'email_on_failure': False,  
    'on_failure_callback': failure_alert, 
    'retries': 0,
}

CONFIG_FILE = os.path.join(BASE_DIR, "config.yaml")

with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)



etl_tasks = config["etl_tasks"]
source_db_var = config["source_db_var"]
target_db_var = config["target_db_var"]

source_db_conf = get_db_config_from_airflow(source_db_var)
target_db_conf = get_db_config_from_airflow(target_db_var)

with DAG(
    dag_id="daphne_ocn_cat_to_dwh",
    default_args=default_args,
    schedule_interval="40 4 * * *",
    catchup=False,
    description="ocn_cat -> dwh ETL (PythonOperator + YAML config)",
    tags= ["daphne","ocn_cat_txn","method:full"]
) as dag:

    for task_conf in etl_tasks:
        task_id = f"{task_conf['source_table']}_to_{task_conf['target_schema']}_{task_conf['target_table']}"

        PythonOperator(
            task_id=task_id,
            python_callable=run_etl_task,
            op_kwargs={
                "task_conf": task_conf,
                "source_db_conf": source_db_conf,
                "target_db_conf": target_db_conf
            }
        )
