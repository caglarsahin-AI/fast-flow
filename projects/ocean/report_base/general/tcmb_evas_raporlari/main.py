import os
import json
import psycopg2
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import oracledb


import warnings

warnings.filterwarnings("ignore")




table_mapping = {
    "acq_resp.sql": "CLEARING.TCMB_REPORT_STAT",
    "iss_resp.sql": "CLEARING.TCMB_REPORT_STAT",
    "stat_tmp_hour.sql": "CLEARING.TCMB_REPORT_STAT_TMP_HOUR",
    "stat_tmp_minute.sql": "CLEARING.TCMB_REPORT_STAT_TMP_MINUTE"
}


table_column_mapping = {
    "acq_resp.sql": ["report_code", "trx_source", "trx_datetime", "bank_code","trx_resp_code", "trx_resp_count"],
    "iss_resp.sql": ["report_code", "trx_source", "trx_datetime", "bank_code","trx_resp_code", "trx_resp_count"],
    "stat_tmp_hour.sql": ["trx_source", "trx_datetime", "trx_hour", "bank_code", "trx_resp_count"],
    "stat_tmp_minute.sql": ["trx_source", "trx_datetime", "bank_code", "trx_resp_count"]
}


def load_env_or_airflow_config(key, is_airflow_env, default_port=5432):
    if is_airflow_env:
        from airflow.models import Variable
        return json.loads(Variable.get(key))
    else:
        load_dotenv()
        return {
            "host": os.getenv(f"{key.upper()}_HOST"),
            "port": int(os.getenv(f"{key.upper()}_PORT", default_port)),
            "database": os.getenv(f"{key.upper()}_DB"),
            "user": os.getenv(f"{key.upper()}_USER"),
            "password": os.getenv(f"{key.upper()}_PASSWORD")
        }

class PostgresConnection:
    def __init__(self, config):
        self.conn = psycopg2.connect(**config)

    def execute_query(self, query):
        return pd.read_sql(query, self.conn)

    def insert_dataframe(self, table, df, insert_columns):
        cursor = self.conn.cursor()
        cols = ', '.join(insert_columns)
        placeholders = ', '.join(['%s'] * len(insert_columns))
        sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
        data = [tuple(row) for row in df[insert_columns].values]
        cursor.executemany(sql, data)
        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()

class OracleConnection:
    def __init__(self, config):
        # pure-Python driver, Oracle Instant Client gerekmez / ?
        dsn = oracledb.makedsn(config['host'], config['port'], service_name=config['database'])
        self.conn = oracledb.connect(
            user=config['user'],
            password=config['password'],
            dsn=dsn,
        )

    def insert_dataframe(self, table, df, insert_columns):
        cursor = self.conn.cursor()
        placeholders = ",".join([":" + str(i + 1) for i in range(len(insert_columns))])
        sql = f"INSERT INTO {table} ({', '.join(insert_columns)}) VALUES ({placeholders})"
        data = [tuple(row) for row in df[insert_columns].values]
        cursor.executemany(sql, data)
        self.conn.commit()
        cursor.close()

    def close(self):
        self.conn.close()

def load_sql(file_name):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()    


def fill_missing_trx_hours(df):
    df['trx_datetime'] = pd.to_datetime(df['trx_datetime'])
    df['trx_datetime'] = df['trx_datetime'].dt.normalize()
    df['trx_date'] = df['trx_datetime'].dt.date  # <-- ekledik

    new_rows = []
    today = datetime.now().date()
    first_day_this_month = today.replace(day=1)
    last_day_prev_month = first_day_this_month - timedelta(days=1)
    start_date = last_day_prev_month.replace(day=1)
    end_date = last_day_prev_month

    print(f"start_date: {start_date}, end_date: {end_date}")

    current_date = start_date
    while current_date <= end_date:
        for hour in range(24):
            exists = ((df['trx_date'] == current_date) & (df['trx_hour'] == hour)).any()

            if not exists:
                new_rows.append({
                    'current_timestamp': df['current_timestamp'].iloc[0],
                    'trx_source': None,
                    'trx_datetime': pd.Timestamp(current_date),
                    'trx_hour': hour,
                    'bank_code': 28,
                    'trx_resp_count': 0
                })
        current_date += timedelta(days=1)

    if new_rows:
        df_new = pd.DataFrame(new_rows)
        df = pd.concat([df, df_new], ignore_index=True)
    df.drop(columns=['trx_date'], inplace=True)
    return df


def run_etl(ocean_config, oracle_config):
    ocean_db = PostgresConnection(ocean_config)
    oracle = OracleConnection(oracle_config)

    try:
        for sql_file in table_mapping:
            query = load_sql(sql_file)
            print(f"[INFO] Executing query from {sql_file}...")
            df = ocean_db.execute_query(query)

            if df.empty:
                print(f"[INFO] No data found for {sql_file}")
                continue

            if sql_file == "stat_tmp_hour.sql":
                df = fill_missing_trx_hours(df)
    
            df['trx_resp_count'] = df['trx_resp_count'].astype(int)
            
            insert_cols = table_column_mapping[sql_file]
            oracle.insert_dataframe(table_mapping[sql_file], df, insert_cols)
            print(f"[INFO] Inserted into {table_mapping[sql_file]}")
    finally:
        ocean_db.close()
        oracle.close()

def main():
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    ocean_config = load_env_or_airflow_config("ocean", is_airflow_env)
    oracle_config = load_env_or_airflow_config("oracle", is_airflow_env, default_port=1521)
    run_etl(ocean_config, oracle_config)

if __name__ == "__main__":
    main()