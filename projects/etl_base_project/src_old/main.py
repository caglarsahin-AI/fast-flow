"""
from etl_manager import ETLManager
from common.config import SOURCE_DB_CONFIG, TARGET_DB_CONFIG

tasks_config = [
    {
        "source_schema": "ocn_cat_txn",
        "source_table": "cat_trnx_log_processing",
        "target_schema": "dwh",
        "target_table": "cat_trnx_log_processing",
        "load_method": "insert_recent_records",
        "date_column": "create_date_time",
        "date_threshold": "20250301"
    },
]

def main():
    etl_manager = ETLManager(SOURCE_DB_CONFIG, TARGET_DB_CONFIG)

    for task_conf in tasks_config:
        print(f"Çalıştırılıyor: {task_conf['source_schema']}.{task_conf['source_table']} → {task_conf['target_schema']}.{task_conf['target_table']}")
        
        etl_manager.run_etl_task(**task_conf)

if __name__ == "__main__":
    main()
"""
