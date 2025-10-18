import os, sys
from send_email_custom import failure_alert 
BASE_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../../projects/daphne/ocn_cat_txn')
)
SRC_DIR = os.path.join(BASE_DIR, 'src')
sys.path.insert(0, SRC_DIR)

import yaml
import json
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from etl_manager import ETLManager  

CONFIG_FILE = os.path.join(BASE_DIR, "config.yaml")

with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

etl_tasks     = config["etl_tasks"]
source_db_var = config["source_db_var"]
target_db_var = config["target_db_var"]

def get_db_config_from_airflow(var_name: str):
    try:
        return json.loads(Variable.get(var_name))
    except KeyError:
        return None

source_db_conf = get_db_config_from_airflow(source_db_var)
target_db_conf = get_db_config_from_airflow(target_db_var)

def run_etl_task(task_conf, source_db_conf, target_db_conf):
    mgr = ETLManager(source_db_conf, target_db_conf)
    mgr.run_etl_task(**task_conf)

local_tz = pendulum.timezone("Europe/Istanbul")
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 14, tzinfo=local_tz),
    'email_on_failure': False,  
    'on_failure_callback': failure_alert, 
    'retries': 0,
}

with DAG(
    dag_id="daphne_ocn_cat_txn_to_dwh",
    default_args=default_args,
    schedule_interval="45 4 * * *",
    catchup=False,
    description="ocn_cat_txn → dwh ETL (PythonOperator + YAML config)",
    tags= ["daphne","ocn_cat_txn","method:delta"]
) as dag:

    task_list = []
    for task_conf in etl_tasks:
        tid = f"{task_conf['source_table']}_to_{task_conf['target_schema']}_{task_conf['target_table']}"
        op = PythonOperator(
            task_id=tid,
            python_callable=run_etl_task,
            op_kwargs={
                "task_conf":      task_conf,
                "source_db_conf": source_db_conf,
                "target_db_conf": target_db_conf
            },
            trigger_rule=TriggerRule.ALL_DONE, 
        )
        task_list.append(op)

    for upstream, downstream in zip(task_list, task_list[1:]):
        upstream >> downstream
