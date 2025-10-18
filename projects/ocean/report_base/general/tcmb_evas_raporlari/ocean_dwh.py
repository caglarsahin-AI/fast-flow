import os
import json
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from pandas.tseries.offsets import MonthEnd

import warnings

warnings.filterwarnings("ignore")


def fill_missing_hours(df):
    df = df.copy()

    df['date'] = df['trx_datetime'].dt.normalize()

    first       = df['date'].iloc[0]
    month_start = first.replace(day=1)
    month_end   = (month_start + MonthEnd()).normalize()
    print(f"month_start: {month_start}, month_end: {month_end}")
    print(f"first date: {first}, last date: {df['date'].iloc[-1]}")
    dates = pd.date_range(month_start, month_end, freq='D')
    print(f"dates: {dates}")
    hours = range(24)

    grid = pd.MultiIndex.from_product(
        [dates, hours],
        names=['date','trx_hour']
    )
    full = pd.DataFrame(index=grid).reset_index()

    full['trx_datetime']   = full['date']                # still dtype datetime64[ns]
    full['trx_datetime']   = full['trx_datetime'].dt.tz_localize(None)

    full['current_timestamp'] = df['current_timestamp'].iloc[0]  # carry one ts
    full['trx_source']        = None
    full['bank_code']         = 28
    full['trx_resp_count']    = 0.0

    real = df[['date','trx_hour','trx_source','bank_code','trx_resp_count','current_timestamp']]
    merged = full.merge(
        real,
        on=['date','trx_hour'],
        how='left',
        suffixes=('','_r')
    )

    for col in ['trx_source','bank_code','trx_resp_count','current_timestamp']:
        merged[col] = merged[f'{col}_r'].combine_first(merged[col])
        merged.drop(columns=[f'{col}_r'], inplace=True)

    out = merged[[
        'current_timestamp',
        'trx_source',
        'trx_datetime',
        'bank_code',
        'trx_hour',
        'trx_resp_count'
    ]]
    return out

table_mapping = {
    "acq_resp.sql": "etl_transform.tcmb_report_stat",
    "iss_resp.sql": "etl_transform.tcmb_report_stat",
    "stat_tmp_hour.sql": "etl_transform.tcmb_report_stat_tmp_hour",
    "stat_tmp_minute.sql": "etl_transform.tcmb_report_stat_tmp_minute"
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

def load_sql(file_name):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()


"""
def transform_dataframe(df):
    #df["report_code"] = df.get("report_code", "ACQ_RESP")

    df["bank_code"] = df["bank_code"].astype("int")
    df["trx_resp_count"] = df["trx_resp_count"].astype("float")
    if "trx_resp_code" in df.columns:
        df["trx_resp_code"] = df["trx_resp_code"].astype("str")
    df["trx_datetime"] = pd.to_datetime(df["trx_datetime"])
    df["trx_source"] = df["trx_source"].astype("str")
    if "trx_hour" in df.columns:
        df["trx_hour"] = df["trx_hour"].astype("int")
    return df
"""

def run_etl(ocean_config, dwh_config):
    ocean_db = PostgresConnection(ocean_config)
    dwh_db = PostgresConnection(dwh_config)

    try:
        for sql_file in table_mapping.keys():
            print(f"[INFO] Processing {sql_file}")
            query = load_sql(sql_file)
            df = ocean_db.execute_query(query)
            if df.empty:
                print(f"[INFO] No data found for {sql_file}")
                continue

            if sql_file == 'stat_tmp_hour.sql':
                df = fill_missing_hours(df)
            
            insert_cols = table_column_mapping[sql_file]
            dwh_db.insert_dataframe(table_mapping[sql_file], df, insert_cols)
            print(f"[INFO] Inserted into {table_mapping[sql_file]}")

    finally:
        ocean_db.close()
        dwh_db.close()

def main():
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None
    ocean_config = load_env_or_airflow_config("ocean", is_airflow_env)
    dwh_config = load_env_or_airflow_config("dwh", is_airflow_env)

    run_etl(ocean_config, dwh_config)

if __name__ == "__main__":
    main()
