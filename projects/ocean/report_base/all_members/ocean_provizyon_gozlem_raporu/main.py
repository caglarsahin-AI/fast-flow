import os
import psycopg2
import warnings
import json
import calendar
import pandas as pd
from io import StringIO
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import csv

warnings.filterwarnings("ignore")

# ==================== UTILITIES ====================

def load_env_or_airflow_variable(variable_key: str, is_airflow_env: bool) -> dict:
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
    return config


class DatabaseConnection:
    def __init__(self, host, port, database, user, password):
        self.conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )

    def execute_query(self, query: str) -> pd.DataFrame:
        return pd.read_sql(query, self.conn)

    def execute_non_query(self, query: str):
        with self.conn.cursor() as cur:
            cur.execute(query)
        self.conn.commit()

    def insert_dataframe(self, df: pd.DataFrame, table_name: str):
        # Float -> Int fix (312321.0 -> 312321)
        for col in df.columns:
            if (pd.api.types.is_float_dtype(df[col])
                and df[col].dropna().astype(int).eq(df[col].dropna()).all()):
                df[col] = df[col].astype('Int64')

        # 1) Buffer’a yaz, artık na_rep kullanmıyoruz
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # 2) Kopyalama SQL’i; artık NULL parametresi yok
        cols = ', '.join(df.columns)
        sql = f"""
        COPY {table_name} ({cols})
        FROM STDIN
        WITH (FORMAT csv)
        """
        with self.conn.cursor() as cur:
            cur.copy_expert(sql, buffer)
        self.conn.commit()

    def close(self):
        self.conn.close()


def load_sql(file_name: str, day_str: str = None):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, 'sql', file_name)
    with open(file_path, 'r', encoding='utf-8') as f:
        sql = f.read()
        if day_str:
            sql = sql.replace("{{etl_date}}", day_str)
        return sql

# ==================== CORE LOGIC ====================

def build_partition_table_name(base: str, dt: date) -> str:
    return f"{base}_{dt.strftime('%Y%m')}"  # edw.ocean_provizyon_gozlem_raporu_YYYYMM


def month_bounds(dt: date):
    start = dt.replace(day=1)
    last_day = calendar.monthrange(dt.year, dt.month)[1]
    end = dt.replace(day=last_day)
    return start, end


def get_months_to_refresh(run_dt: date, first_n_days: int = 10):
    first_of_month = run_dt.replace(day=1)
    if run_dt.day <= first_n_days:
        prev_month = (first_of_month - timedelta(days=1)).replace(day=1)
        return [prev_month, first_of_month]
    else:
        return [first_of_month]


def truncate_partition_tables(dwh_db: DatabaseConnection, base_table: str, months: list[date]):
    for m in months:
        part_tbl = build_partition_table_name(base_table, m)
        dwh_db.execute_non_query(f"TRUNCATE TABLE {part_tbl};")
        print(f"[INFO] Truncated {part_tbl}")


def run_daily_etl_serial(day_str: str,ocean_config: dict,dwh_config: dict,base_table: str):
    print(f"  -> ETL for {day_str}")
    ocean_db = DatabaseConnection(**ocean_config)
    dwh_db = DatabaseConnection(**dwh_config)

    try:
        base_query = load_sql('select_source_data.sql', day_str)
        df = ocean_db.execute_query(base_query)

        if df.empty:
            print(f"     No data for {day_str}.")
            return

        df['etl_date_time'] = datetime.now().date()
        print(f"     Loaded {len(df)} rows from source for {day_str}.")
        dwh_db.insert_dataframe(df, base_table)
        print(f"     Inserted {len(df)} rows into {base_table} for {day_str}.")
    finally:
        ocean_db.close()
        dwh_db.close()


def load_month_with_daily_loop(month_dt: date,
                               ocean_config: dict,
                               dwh_config: dict,
                               base_table: str):
    start_day, end_day = month_bounds(month_dt)
    current_day = start_day
    while current_day <= end_day:
        day_str = current_day.strftime("%Y%m%d")
        run_daily_etl_serial(day_str, ocean_config, dwh_config, base_table)
        current_day += timedelta(days=1)


def main():
    print(f"[INFO] Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    is_airflow_env = os.getenv("AIRFLOW_CTX_DAG_ID") is not None

    ocean_config = load_env_or_airflow_variable("ocean" if is_airflow_env else "SOURCE_OCEAN_DB", is_airflow_env)
    dwh_config   = load_env_or_airflow_variable("dwh"   if is_airflow_env else "TARGET_DB",       is_airflow_env)

    run_today = datetime.today().date()
    print(f"[INFO] Running ETL for date: {run_today.strftime('%Y-%m-%d')}")
    months = get_months_to_refresh(run_today, first_n_days=10)
    print(f"[INFO] Months to refresh: {[m.strftime('%Y-%m') for m in months]}")
    base_table = "edw.ocean_provizyon_gozlem_raporu"

    dwh_db = DatabaseConnection(**dwh_config)
    try:
        truncate_partition_tables(dwh_db, base_table, months)
    finally:
        dwh_db.close()

    for m in months:
        print(f"\n=== Loading month {m.strftime('%Y-%m')} ===")
        load_month_with_daily_loop(m, ocean_config, dwh_config, base_table)

    print("\n[INFO] ETL Finished.")


if __name__ == "__main__":
    main()
