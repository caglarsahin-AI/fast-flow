#ocn_iss_to_stg_level1_group_1_dag
import os, json, yaml
from datetime import datetime
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from projects.etl_base_project.src.utilities.etl_manager import ETLManager
from projects.etl_base_project.src.utilities.worker import run_partition_worker
from projects.etl_base_project.src.utilities.partition_strategy import plan_partitions 
from projects.etl_base_project.src.common.logger import logger


CURRENT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.abspath(
    os.path.join(
        CURRENT_DIR,   # dags/ocean/ocn_iss/level1/src_to_ods
        "../../../../..",  # dags klasöründen çık
        "projects/ocean/ocn_iss/level1/src_to_stg"
    )
)
CONFIG_FILE = os.path.join(CONFIG_PATH, "config_group_1.yaml")


with open(CONFIG_FILE, "r") as f:
    config = yaml.safe_load(f)

etl_tasks     = config["etl_tasks"]
source_db_var = config["source_db_var"]
target_db_var = config["target_db_var"]

def validate_config(config):
    required = ["etl_tasks", "source_db_var", "target_db_var"]
    allowed_source_types = {"table", "sql", "csv"}
    for key in required:
        if key not in config:
            raise ValueError(f"Config eksik alan: {key}")
    
    for task in config["etl_tasks"]:
        st = task.get("source_type")
        #task_group_id = (task.get("task_group_id") or "").strip()
        col_mode = (task.get("column_mapping_mode") or "source").lower()

        #task_dir = os.path.join(CONFIG_PATH, task_group_id)
        #mapping_path = os.path.join(task_dir, "mapping.yaml")
        mapping_given = (task.get("mapping_file") or "").strip()
        mapping_path = os.path.join(os.getcwd(), mapping_given)
        sql_given = (task.get("sql_file") or "").strip()
        sql_path = os.path.join(os.getcwd(), sql_given)

        if not st:
            raise ValueError(f"source_type not empty Task: {task.get('task_group_id')}")
        st = st.lower()
        if st not in allowed_source_types:
             raise ValueError(f"[task#] Geçersiz source_type: {st}. "
                         f"Beklenen: {sorted(allowed_source_types)}. Task: {task.get('task_group_id')}")
        
        if st == "table":
            if col_mode == "mapping_file" and not os.path.exists(mapping_path):
                raise ValueError(f"Missing mapping file for Table source_type: {mapping_path}")


            req_fields = ["source_schema","source_table", "target_schema", 
                        "target_table", "load_method"]
            for f in req_fields:
                if f not in task:
                    raise ValueError(f"Task eksik alan: {f} - {task.get('source_table')}- {task.get('target_table')}")
        elif st == "sql": # sql ise mapping path ve sql path adreslerinde dosyalar bulunuyor olmalı

            if not os.path.exists(mapping_path):
                raise ValueError(f"Missing mapping file: {mapping_path}")
            if not os.path.exists(sql_path):
                raise ValueError(f"Missing SQL file: {sql_path}")
            req_fields = ["task_group_id", "column_mapping_mode", "target_schema", 
                        "target_table", "load_method"]
            for f in req_fields:
                if f not in task:
                    raise ValueError(f"Task eksik alan: {f} - {task.get('source_table')}- {task.get('target_table')}")
        elif st == "csv":
            req_fields = ["task_group_id", "source_type", "target_schema", 
                        "target_table", "load_method"]
            for f in req_fields:
                if f not in task:
                    raise ValueError(f"Task eksik alan: {f} - {task.get('source_table')}- {task.get('target_table')}")


def wait_for_debug():
    if os.getenv("ENABLE_DEBUG") == "1":
        import debugpy
        if not debugpy.is_client_connected():
            debugpy.listen(("0.0.0.0", 5678))
            print("🔎 Waiting for VS Code debugger on :5678 ...")
            debugpy.wait_for_client()
            debugpy.breakpoint()

def get_db_config_from_airflow(var_name: str):
    try:
        return json.loads(Variable.get(var_name))
    except KeyError:
        return None

source_db_conf = get_db_config_from_airflow(source_db_var)
target_db_conf = get_db_config_from_airflow(target_db_var)

def prepare_target(task_conf, source_db_conf, target_db_conf):
    #wait_for_debug() 
    mgr = ETLManager(source_db_conf, target_db_conf)
    pc = dict(task_conf); pc["prepare_only"] = True
    mgr.run_etl_task(**pc)


local_tz = pendulum.timezone("Europe/Istanbul")
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 2, 14, tzinfo=local_tz),
    "email_on_failure": False,
    "retries": 0,
}

validate_config(config)

with DAG(
    dag_id="ocn_iss_to_stg_level1_group_1",
    default_args=default_args,
    schedule_interval="30 3 * * *",
    catchup=False,
    description="ocn_iss → dwh ETL (Strategy partition)",
    tags=["ocean", "ocn_iss", "src_to_stg", "level1_group_1", "delta"],
) as dag:
    for task_conf in etl_tasks:
        base_id = task_conf.get('task_group_id') or task_conf.get('source_table')
        group_id = f"{base_id}_to_{task_conf['target_schema']}_{task_conf['target_table']}"
        with TaskGroup(group_id=group_id) as tg:

            # 1) Plan (Strategy üzerinden)
            def _plan_callable(task_conf=task_conf):
                # YAML’daki template + bindings
                base_where_tpl = (task_conf.get("where") or "").strip() or None
                
                bindings_conf  = task_conf.get("bindings")
                order_by = (task_conf.get("order_by") or "").strip()
                source_type = (task_conf.get("source_type") or "table").lower()
                
                #wait_for_debug() 

                source_sql = None
                if source_type == "sql":
                    sql_file = task_conf.get("sql_file")
                    with open(sql_file, "r", encoding="utf-8") as f:
                        source_sql = f.read()
                
                parts = plan_partitions(
                    source_db_conf,
                    task_conf.get("source_schema"),
                    task_conf.get("source_table"),
                    task_conf.get("partitioning"),
                    base_where_template=base_where_tpl,
                    bindings=bindings_conf,
                    target_db_conf=target_db_conf,
                    airflow_vars_get=lambda k: Variable.get(k, default_var=None),
                    source_sql=source_sql
                )

                logger.info("DAG 2 base_where_tpl= %s",base_where_tpl)

                out = []
                for p in parts:
                    out.append({
                        "task_conf": {k: v for k, v in task_conf.items() if k != "bindings"},  # runtime'a bindings taşımayalım
                        "where": p.where,          # ← zaten resolved & push-down edilmiş
                        "label": p.label,
                        "order_by": order_by,
                        "sql_text": source_sql
                    })
                    logger.info("DAG 3 base_where_tpl= %s",p.where)
                return out

            
            plan_op = PythonOperator(
                task_id="plan_partitions",
                python_callable=_plan_callable,
            )

            
            # 2) Prepare (DDL/temizlik)
            prepare_op = PythonOperator(
                task_id="prepare_target",
                python_callable=prepare_target,
                op_kwargs={"task_conf": task_conf,
                           "source_db_conf": source_db_conf,
                           "target_db_conf": target_db_conf},
            )

            # 3) Parçaların çalıştırılması (dynamic mapping)
            # DAG içinde
            
            
            part_runner = (
                PythonOperator
                .partial(
                    task_id="run_partition",
                    python_callable=run_partition_worker,
                    op_args=[source_db_conf, target_db_conf],   # sabitler
                    # DİKKAT: burada op_kwargs VERME!
                )
                .expand(op_kwargs=plan_op.output)               # <- doğru kullanım
            )



            
            plan_op >> prepare_op >> part_runner
