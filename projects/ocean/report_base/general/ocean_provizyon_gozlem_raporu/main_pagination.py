import os
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
import threading
from psycopg2.extras import execute_values
import multiprocessing
multiprocessing.current_process().daemon=False
import queue
import warnings
import json
from dotenv import load_dotenv

warnings.filterwarnings("ignore")


def load_env_or_airflow_variable(variable_key: str, is_airflow_env: bool) -> dict:
    #print(f"[INFO] Loading configuration for: {variable_key} (Airflow: {is_airflow_env})")
    if is_airflow_env:
        from airflow.models import Variable
        
        config = json.loads(Variable.get(variable_key))
    else:
        load_dotenv()
        config = {
            "host": os.getenv(f"{variable_key.upper()}_HOST"),
            "port": os.getenv(f"{variable_key.upper()}_PORT"),
            "database": os.getenv(f"{variable_key.upper()}_NAME"),
            "user": os.getenv(f"{variable_key.upper()}_USER"),
            "password": os.getenv(f"{variable_key.upper()}_PASSWORD"),
        }
    #print(f"[INFO] Loaded config: host={config['host']} db={config['database']}")
    return config


class DatabaseConnection:
    def __init__(self, host, port, database, user, password):
        #print(f"[INFO] Connecting to DB: {host}:{port}/{database} as {user}")
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        #print("[INFO] Connected.")

    def execute_query(self, query: str) -> pd.DataFrame:
        #print("[INFO] Executing SELECT query.")
        return pd.read_sql(query, self.conn)

    def execute_non_query(self, query: str):
        #print("[INFO] Executing NON-SELECT query.")
        with self.conn.cursor() as cur:
            cur.execute(query)
            self.conn.commit()

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        #print(f"[INFO] Inserting {len(df)} rows into table: {table_name}")
        with self.conn.cursor() as cur:
            columns = ', '.join(df.columns)
            values = [tuple(None if pd.isna(val) else val for val in row) for row in df.itertuples(index=False)]
            sql = f"INSERT INTO {table_name} ({columns}) VALUES %s"
            execute_values(cur, sql, values)
            self.conn.commit()


    def close(self):
        #print("[INFO] Closing DB connection.")
        self.conn.close()

def load_sql(file_name: str, day_str: str = None):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)
    #print(f"[INFO] Loading SQL from: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f.read()
        if day_str:
            sql = sql.replace("{{etl_date}}", day_str)
        return sql

def run_daily_etl(day_str, source_config, target_config):
    print(f"\n=== 🗓️ Starting ETL for {day_str} ===")

    source_db = DatabaseConnection(**source_config)
    target_db = DatabaseConnection(**target_config)

    try:
        delete_sql = load_sql('delete_old_data.sql', day_str)
        target_db.execute_non_query(delete_sql)
        print(f"[INFO] Deleted old data for {day_str} from target database.")

        base_query = load_sql('select_source_data.sql', day_str)
        count_sql = f"SELECT COUNT(*) FROM ({base_query}) AS subquery"
        total_rows = source_db.execute_query(count_sql).iloc[0, 0]
        page_size = 20000
        total_pages = (total_rows + page_size - 1) // page_size

        print(f"[INFO] {day_str} -> {total_rows} total rows across {total_pages} pages.")

        data_queue = queue.Queue(maxsize=3)
        stop_signal = object()

        def reader():
            for page in range(total_pages):
                offset = page * page_size
                paginated_sql = f"""
                    {base_query}
                    LIMIT {page_size} OFFSET {offset}
                """
                print(f"[READER-{day_str}] Reading page {page + 1}/{total_pages} (OFFSET {offset})")
                df = source_db.execute_query(paginated_sql)
                if df.empty:
                    continue
                df['etl_date_time'] = datetime.now().date()
                data_queue.put(df)
            data_queue.put(stop_signal)
            #print(f"[READER-{day_str}] Finished.")

        def writer():
            while True:
                item = data_queue.get()
                if item is stop_signal:
                    #print(f"[WRITER-{day_str}] Stop signal received.")
                    break
                target_db.insert_dataframe(item, 'edw.ocean_provizyon_gozlem_raporu')
                #print(f"[WRITER-{day_str}] Inserted batch of {len(item)} rows")

        t_read = threading.Thread(target=reader)
        t_write = threading.Thread(target=writer)

        t_read.start()
        t_write.start()
        t_read.join()
        t_write.join()

        print(f"=== ✅ Finished ETL for {day_str} ===")

    finally:
        source_db.close()
        target_db.close()

def worker_launcher(task_queue, source_config, target_config):
    while not task_queue.empty():
        try:
            day_str = task_queue.get_nowait()
            run_daily_etl(day_str, source_config, target_config)
        except queue.Empty:
            break

"""
def main():
    print(f"[INFO] Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    source_config = load_env_or_airflow_variable("ocean" if is_airflow_env else "SOURCE_OCEAN_DB", is_airflow_env)
    target_config = load_env_or_airflow_variable("dwh" if is_airflow_env else "TARGET_DB", is_airflow_env)

    days_to_process = 90
    max_concurrent_processes = 3

    
    today = datetime.today()
    task_queue = multiprocessing.Queue()
    for i in range(days_to_process, 0, -1):
        day = today - timedelta(days=i)
        day_str = day.strftime("%Y%m%d")
        task_queue.put(day_str)

    processes = []
    for _ in range(max_concurrent_processes):
        p = multiprocessing.Process(
            target=worker_launcher,
            args=(task_queue, source_config, target_config)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
"""

def main():
    print(f"[INFO] Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    source_config = load_env_or_airflow_variable("ocean" if is_airflow_env else "SOURCE_OCEAN_DB", is_airflow_env)
    target_config = load_env_or_airflow_variable("dwh" if is_airflow_env else "TARGET_DB", is_airflow_env)

    start_date = datetime.strptime("2024-06-01", "%Y-%m-%d")
    end_date = datetime.strptime("2025-05-01", "%Y-%m-%d")

    max_concurrent_processes = 3

    task_queue = multiprocessing.Queue()
    current_day = start_date
    while current_day <= end_date:
        day_str = current_day.strftime("%Y%m%d")
        task_queue.put(day_str)
        current_day += timedelta(days=1)

    processes = []
    for _ in range(max_concurrent_processes):
        p = multiprocessing.Process(
            target=worker_launcher,
            args=(task_queue, source_config, target_config)
        )
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
        
if __name__ == "__main__":
    #multiprocessing.freeze_support() -> SADECE WINDOWS ! LINUX DA GEREK YOK.
    main()
