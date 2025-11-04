# dags/mssql_migration_dag.py
import os, sys
from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from send_email_custom import failure_alert

local_tz = pendulum.timezone("Europe/Istanbul")

# --- PATH REVISION (new layout) ---
DAGS_DIR = os.path.dirname(__file__)
# .../dags/migration/mainframe_migration  ->  .../projects
PROJECTS_DIR = os.path.abspath(os.path.join(DAGS_DIR, "..", "..", "..", "projects"))

# .../projects/migration/mainframe_migration/clean_miss_match
MIGRATOR_DIR = os.path.join(
    PROJECTS_DIR,
    "migration",
    "mainframe_migration",
    "dw_pp_migration",
)

if MIGRATOR_DIR not in sys.path:
    sys.path.insert(0, MIGRATOR_DIR)



# Artık direkt main.py içinden import edebiliriz
from main import main as run_migration_main

def run_migration_task(**context):
    # Airflow Variable yolunu kullandırmak için bayrak
    os.environ["AIRFLOW_CTX_DAG_ID"] = context["dag"].dag_id
    # Çalıştır
    run_migration_main()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1, tzinfo=local_tz),
    "email_on_failure": False,
    "on_failure_callback": failure_alert,
    "retries": 0,
}

with DAG(
    dag_id="dw_pp_migration_dag",
    default_args=default_args,
    schedule_interval=None,   # elle tetikle
    catchup=False,
    tags=["mert", "migration", "mssql", "postgresql"],
) as dag:

    run_migration = PythonOperator(
        task_id="run_migration_tblIso8583",
        python_callable=run_migration_task,
        provide_context=True,
    )
