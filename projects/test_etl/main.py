import os
import json
from dotenv import load_dotenv
import psycopg2
import pandas as pd


def load_sql(file_name):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()
    
class PostgresConnection:
    def __init__(self, config):
        self.conn = psycopg2.connect(**config)

    def execute_query(self, query):
        return pd.read_sql(query, self.conn)
    
    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        print(f"[INFO] Inserting {len(df)} rows into table: {table_name}")
        with self.conn.cursor() as cur:
            for _, row in df.iterrows():
                columns = ', '.join(df.columns)
                placeholders = ', '.join(['%s'] * len(row))
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                values = [None if pd.isna(val) else val for val in row]
                cur.execute(sql, values)
            self.conn.commit()
        print("[INFO] Insert completed.")

    def close(self):
        self.conn.close()

def load_env_or_airflow_config(key, is_airflow_env):
    if is_airflow_env:
        from airflow.models import Variable
        return json.loads(Variable.get(key))
    else:
        load_dotenv()
        return {
            "host": os.getenv(f"{key.upper()}_HOST"),
            "port": os.getenv(f"{key.upper()}_PORT"),
            "database": os.getenv(f"{key.upper()}_NAME"),
            "user": os.getenv(f"{key.upper()}_USER"),
            "password": os.getenv(f"{key.upper()}_PASSWORD")
        }


def saim_reis_first_main():
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None

    dwh_config = load_env_or_airflow_config("target_db", is_airflow_env)
    ocean_config = load_env_or_airflow_config("source_ocean_db", is_airflow_env)

    ocean_db_class = PostgresConnection(ocean_config)
    #dwh_db_class = PostgresConnection(dwh_config)

    my_select_sql = load_sql("basic.sql")

    data = ocean_db_class.execute_query(my_select_sql)
    print(data)


if __name__ == "__main__":
    saim_reis_first_main()